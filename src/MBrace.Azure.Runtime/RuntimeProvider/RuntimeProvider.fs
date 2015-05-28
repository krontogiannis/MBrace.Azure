﻿namespace MBrace.Azure.Runtime

#nowarn "444"

open System

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Core.Internals.InMemoryRuntime
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Info
open MBrace.Azure.Runtime.Primitives
        
/// Scheduling implementation provider
type RuntimeProvider private (state : RuntimeState, job : JobItem, dependencies, isForcedLocalParallelism : bool) =

    let mkNestedCts (ct : ICloudCancellationToken) =
        let parentCts = ct :?> DistributedCancellationTokenSource
        let dcts = state.ResourceFactory.RequestCancellationTokenSource(job.ProcessInfo.Id, parent = parentCts, elevate = false)
                   |> Async.RunSync
        dcts :> ICloudCancellationTokenSource

    /// Creates a runtime provider instance for a provided job
    static member FromJob state dependencies (job : JobItem) =
        new RuntimeProvider(state, job, dependencies, false)

    interface IDistributionProvider with
        member __.CreateLinkedCancellationTokenSource(parents: ICloudCancellationToken []): Async<ICloudCancellationTokenSource> = 
            async {
                match parents with
                | [||] -> 
                    let! cts = state.ResourceFactory.RequestCancellationTokenSource(job.ProcessInfo.Id, elevate = false) 
                    return cts :> ICloudCancellationTokenSource
                | [| ct |] -> return mkNestedCts ct
                | _ -> return raise <| new System.NotSupportedException("Linking multiple cancellation tokens not supported in this runtime.")
            }
        member __.ProcessId = job.ProcessInfo.Id

        member __.JobId = job.JobId

        member __.FaultPolicy = job.FaultPolicy
        member __.WithFaultPolicy newPolicy = 
            let job' = { job with FaultPolicy = newPolicy }
            new RuntimeProvider(state, job', dependencies, isForcedLocalParallelism) :> IDistributionProvider

        member __.IsForcedLocalParallelismEnabled = isForcedLocalParallelism
        member __.WithForcedLocalParallelismSetting setting =
            new RuntimeProvider(state, job, dependencies, setting) :> IDistributionProvider

        member __.IsTargetedWorkerSupported = true

        member __.ScheduleLocalParallel computations = ThreadPool.Parallel(mkNestedCts, computations)
        member __.ScheduleLocalChoice computations = ThreadPool.Choice(mkNestedCts, computations)

        member __.ScheduleParallel computations = cloud {
            if isForcedLocalParallelism then
                return! ThreadPool.Parallel(mkNestedCts, computations |> Seq.map fst)
            else
                // Temporary for EnqueueBatch ServiceBus limitations.
                if Seq.length computations > 1024 then
                    return! Cloud.Raise(ArgumentOutOfRangeException("computations", "Limit reached."))
                else
                    return! Combinators.Parallel state job dependencies computations
        }

        member __.ScheduleChoice computations = cloud {
            if isForcedLocalParallelism then
                return! ThreadPool.Choice(mkNestedCts, (computations |> Seq.map fst))
            else
                if Seq.length computations > 1024 then
                    return! Cloud.Raise(ArgumentOutOfRangeException("computations", "Limit reached."))
                else
                return! Combinators.Choice state job dependencies computations
        }

        member __.ScheduleStartAsTask(workflow : Cloud<'T>, faultPolicy, ?cancellationToken, ?target:IWorkerRef) =
           Combinators.StartAsCloudTask state job.ProcessInfo job.JobId dependencies cancellationToken faultPolicy workflow target

        member __.GetAvailableWorkers () = async { 
            let! ws = state.WorkerManager.GetWorkerRefs(
                        WorkerManager.MaxHeartbeatTimeSpan, 
                        showStarting = false, 
                        showInactive = false, 
                        showFaulted = false)
            return ws |> Seq.map (fun w -> w :> IWorkerRef)
                      |> Seq.toArray 
            }
        member __.CurrentWorker =
            let w = state.WorkerManager.Current
            new MBrace.Azure.WorkerRef(Configuration.Pickler.UnPickle(w.ConfigurationId) ,w.PartitionKey, w.RowKey) :> IWorkerRef
        member __.Logger = state.ResourceFactory.RequestProcessLogger(job.ProcessInfo.Id) 
