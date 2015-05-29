﻿module MBrace.Azure.Runtime.Combinators

//
//  Provides distributed implementations for Cloud.Parallel, Cloud.Choice and Cloud.StartChild
//

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Primitives
open MBrace.Azure
open MBrace.Azure.Runtime.Info

#nowarn "444"

let inline private withCancellationToken (cts : ICloudCancellationToken) (ctx : ExecutionContext) =
    { ctx with Resources = ctx.Resources.Register(cts) ; CancellationToken = cts }

let private asyncFromContinuations f =
    Cloud.FromContinuations(fun ctx cont -> JobExecutionMonitor.ProtectAsync ctx (f ctx cont))
        
let Parallel (state : RuntimeState) (parentJob : JobItem) dependencies (computations : seq<#Cloud<'T> * IWorkerRef option>) =
    asyncFromContinuations(fun ctx cont -> async {
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
        | Choice1Of2 [||] -> cont.Success ctx [||]
        // schedule single-child parallel workflows in current job
        // force copy semantics by cloning the workflow
        | Choice1Of2 [| (comp, None) |] ->
            let (comp, cont) = Configuration.Pickler.Clone (comp, cont)
            let cont' = Continuation.map (fun t -> [| t |]) cont
            Cloud.StartWithContinuations(comp, cont', ctx)

        | Choice1Of2 computations ->
            // request runtime resources required for distribution coordination
            let currentCts = ctx.CancellationToken :?> DistributedCancellationTokenSource
            
            let pid = parentJob.ProcessInfo.Id
            let parentId = parentJob.JobId
            let workerId = state.WorkerManager.Current.Id
            let! childCts = state.ResourceFactory.RequestCancellationTokenSource(pid, parent = currentCts, metadata = parentId, elevate = true)
            
            let requestBatch = state.ResourceFactory.GetResourceBatchForProcess(pid)
            let resultAggregator = requestBatch.RequestResultAggregator<'T>(computations.Length)
            let cancellationLatch = requestBatch.RequestCounter(0)
            do! requestBatch.CommitAsync()
            
            let onSuccess i ctx (t : 'T) = 
                async { 
                    let! isCompleted = resultAggregator.SetResult(i, t)
                    if isCompleted then 
                        // this is the last child callback, aggregate result and call parent continuation
                        let! results = resultAggregator.ToArray()
                        childCts.Cancel()
                        cont.Success (withCancellationToken currentCts ctx) results
                    else // results pending, declare task completed.
                        JobExecutionMonitor.TriggerCompletion ctx
                } |> JobExecutionMonitor.ProtectAsync ctx

            let onException ctx e =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first task to request workflow cancellation, grant access
                        childCts.Cancel()
                        cont.Exception (withCancellationToken currentCts ctx) e
                    else // cancellation already triggered by different party, declare task completed.
                        JobExecutionMonitor.TriggerCompletion ctx
                } |> JobExecutionMonitor.ProtectAsync ctx

            let onCancellation ctx c =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first task to request workflow cancellation, grant access
                        childCts.Cancel()
                        cont.Cancellation ctx c
                    else // cancellation already triggered by different party, declare task completed.
                        JobExecutionMonitor.TriggerCompletion ctx
                } |> JobExecutionMonitor.ProtectAsync ctx

            try
                do! state.EnqueueJobBatch(parentJob.ProcessInfo, dependencies, childCts, parentJob.FaultPolicy, onSuccess, onException, onCancellation, computations, DistributionType.Parallel, parentJob.JobId, parentJob.ParentResultCell, resultAggregator.PartitionKey, resultAggregator.RowKey)
            with e ->
                childCts.Cancel() ; return! Async.Raise e
             
            JobExecutionMonitor.TriggerCompletion ctx })

let Choice (state : RuntimeState) (parentJob : JobItem) dependencies (computations : seq<#Cloud<'T option> * IWorkerRef option>)  =
    asyncFromContinuations(fun ctx cont -> async {
        match (try Seq.toArray computations |> Choice1Of2 with e -> Choice2Of2 e) with
        | Choice2Of2 e -> cont.Exception ctx (ExceptionDispatchInfo.Capture e)
        | Choice1Of2 [||] -> cont.Success ctx None
        // schedule single-child parallel workflows in current job
        // force copy semantics by cloning the workflow
        | Choice1Of2 [| (comp, None) |] ->
            let (comp, cont) = Configuration.Pickler.Clone (comp, cont)
            Cloud.StartWithContinuations(comp, cont, ctx)

        | Choice1Of2 computations ->
            // request runtime resources required for distribution coordination
            let pid = parentJob.ProcessInfo.Id
            let parentId = parentJob.JobId
            let workerId = state.WorkerManager.Current.Id
            
            let n = computations.Length // avoid capturing computation array in cont closures
            let currentCts = ctx.CancellationToken :?> DistributedCancellationTokenSource
            let! childCts = state.ResourceFactory.RequestCancellationTokenSource(parentJob.JobId, parent = currentCts, metadata = parentJob.JobId, elevate = true)
            
            let batchRequest = state.ResourceFactory.GetResourceBatchForProcess(parentJob.ProcessInfo.Id)
            let completionLatch = batchRequest.RequestCounter(0)
            let cancellationLatch = batchRequest.RequestCounter(0)
            do! batchRequest.CommitAsync()

            let onSuccess ctx (topt : 'T option) =
                async {
                    if Option.isSome topt then // 'Some' result, attempt to complete workflow
                        let! latchCount = cancellationLatch.Increment()
                        if latchCount = 1 then 
                            // first child to initiate cancellation, grant access to parent scont
                            childCts.Cancel ()
                            cont.Success (withCancellationToken currentCts ctx) topt
                        else
                            // workflow already cancelled, declare task completion
                            JobExecutionMonitor.TriggerCompletion ctx
                    else
                        // 'None', increment completion latch
                        let! completionCount = completionLatch.Increment ()
                        if completionCount = n then 
                            // is last task to complete with 'None', pass None to parent scont
                            childCts.Cancel()
                            cont.Success (withCancellationToken currentCts ctx) None
                        else
                            // other tasks pending, declare task completion
                            JobExecutionMonitor.TriggerCompletion ctx
                } |> JobExecutionMonitor.ProtectAsync ctx

            let onException ctx e =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first task to request workflow cancellation, grant access
                        childCts.Cancel ()
                        cont.Exception (withCancellationToken currentCts ctx) e
                    else // cancellation already triggered by different party, declare task completed.
                        JobExecutionMonitor.TriggerCompletion ctx
                } |> JobExecutionMonitor.ProtectAsync ctx

            let onCancellation ctx c =
                async {
                    let! latchCount = cancellationLatch.Increment()
                    if latchCount = 1 then // is first task to request workflow cancellation, grant access
                        childCts.Cancel()
                        cont.Cancellation (withCancellationToken currentCts ctx) c
                    else // cancellation already triggered by different party, declare task completed.
                        JobExecutionMonitor.TriggerCompletion ctx
                } |> JobExecutionMonitor.ProtectAsync ctx

            try
                do! state.EnqueueJobBatch(parentJob.ProcessInfo, dependencies, childCts, parentJob.FaultPolicy, (fun _ -> onSuccess), onException, onCancellation, computations, DistributionType.Choice, parentJob.JobId, parentJob.ParentResultCell, null, null)
            with e ->
                childCts.Cancel() ; return! Async.Raise e
                    
            JobExecutionMonitor.TriggerCompletion ctx })


let StartAsCloudTask (state : RuntimeState) psInfo (jobId : string) dependencies ct fp (computation : Cloud<'T>) (affinity : IWorkerRef option) = cloud {
    let taskType = match affinity with None -> JobType.Task | Some wr -> JobType.TaskAffined wr.Id
    return! Cloud.OfAsync <| state.StartAsTask(psInfo, dependencies, ct, fp, computation, taskType, jobId)
}