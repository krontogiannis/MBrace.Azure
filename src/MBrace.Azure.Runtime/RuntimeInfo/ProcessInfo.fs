﻿namespace MBrace.Azure.Runtime.Info

open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table
open MBrace
open MBrace.Azure.Runtime
open System
open System.Diagnostics
open System.Net
open System.Runtime.Serialization
open Nessos.Vagabond
open MBrace.Azure
open MBrace.Azure.Runtime.Primitives
open MBrace.Azure.Runtime.Utilities
open Microsoft.FSharp.Linq.NullableOperators

/// Represents the current stage in the lifecycle of a Process.
type ProcessStatus = 
    /// Successfully posted to the Runtime Queue.
    | Posted = 0
    /// Process is running but has not yet completed.
    | Running = 1
    /// Process completed due to an unrecoverable FaultException.
    | Faulted = 2
    /// Process completed execution successfully.
    | Completed = 3
    /// Acknowledged cancellation by throwing an OperationCanceledException.
    | Canceled = 4
    /// Cancellation is requested for this process.
    | CancellationRequested = 5

type ProcessRecord(pk, pid, pname, cancellationPK, cancellationRK, state, resultUri, ty, typeName, deps) = 
    inherit TableEntity(pk, pid)
    member val Id  : string = pid with get, set
    member val Name : string = pname with get, set

    member val Status : Nullable<int> = state with get, set
    member val InitializationTime = Nullable<DateTimeOffset>() with get, set
    member val CompletionTime     = Nullable<DateTimeOffset>() with get, set
    member val Completed          = Nullable<bool>() with get, set
    
    member val ResultRowKey : string = resultUri with get, set
    member val CancellationPartitionKey : string = cancellationPK with get, set
    member val CancellationRowKey : string = cancellationRK with get, set
    
//    member val TotalJobs     = Nullable<int>() with get, set
//    member val ActiveJobs    = Nullable<int>() with get, set
//    member val CompletedJobs = Nullable<int>() with get, set
//    member val FaultedJobs   = Nullable<int>() with get, set

    member val TypeName : string = typeName with get, set
    member val Type : byte [] = ty with get, set
    member val Dependencies : byte [] = deps with get, set
    member __.UnpickleType () = Configuration.Pickler.UnPickle<Type> __.Type
    member __.UnpickleDependencies () = Configuration.Pickler.UnPickle<AssemblyId list> __.Dependencies
    
    new () = new ProcessRecord(null, null, null, null, null, nullableDefault, null, null, null, null)

    member this.CloneDefault() =
        let p = new ProcessRecord()
        p.PartitionKey <- this.PartitionKey
        p.RowKey <- this.RowKey
        p.ETag <- this.ETag
        p

type ProcessManager private (config : ConfigurationId) = 
    let pk = "ProcessInfo"
    let table = config.RuntimeTable
    
    static member Create(configId : ConfigurationId) = new ProcessManager(configId)

    member this.CreateRecord(pid : string, name, ty : Type, deps : AssemblyId list, cts : DistributedCancellationTokenSource, resultRowKey) = 
        async {
            let now = DateTimeOffset.UtcNow
            let pickledTy = Configuration.Pickler.Pickle(ty)
            let deps = Configuration.Pickler.Pickle(deps)
            let tyName = Runtime.Utils.PrettyPrinters.Type.prettyPrint ty
            let ctsRK =
                cts.ElevateCancellationToken() |> ignore
                match cts.RowKey with
                | None -> raise <| new OperationCanceledException()
                | Some rK -> rK
            let status = nullable <| int ProcessStatus.Posted
            let e = new ProcessRecord(pk, pid, name, cts.PartitionKey, ctsRK, status, resultRowKey, pickledTy, tyName, deps)
//            e.ActiveJobs <- nullable 0
//            e.FaultedJobs <- nullable 0
//            e.CompletedJobs <- nullable 0
//            e.TotalJobs <- nullable 0
            e.Completed <- nullable false
            e.InitializationTime <- nullable now
            return! Table.insert config config.RuntimeTable e
        }

//    member this.IncreaseTotalJobs(pid : string, ?count) = 
//        let count = defaultArg count 1
//        Table.transact2<ProcessRecord> config table pk pid 
//            (fun pr ->  
//                let p = pr.CloneDefault()
//                p.TotalJobs <- pr.TotalJobs ?+ count
//                p)
//        |> Async.Ignore
//
//    member this.AddActiveJob(pid : string) = 
//        Table.transact2<ProcessRecord> config table pk pid 
//            (fun pr -> 
//                let p = pr.CloneDefault()
//                p.ActiveJobs <- pr.ActiveJobs ?+ 1
//                p)
//        |> Async.Ignore
//
//    member this.AddFaultedJob(pid : string) = 
//        Table.transact2<ProcessRecord> config table pk pid 
//            (fun pr -> 
//                let p = pr.CloneDefault()
//                p.ActiveJobs <- pr.ActiveJobs ?- 1
//                p.FaultedJobs <- pr.FaultedJobs ?+ 1
//                p)
//        |> Async.Ignore
//
//    member this.AddCompletedJob(pid : string) = 
//        Table.transact2<ProcessRecord> config table pk pid 
//            (fun pr -> 
//                let p = pr.CloneDefault()
//                p.ActiveJobs <- pr.ActiveJobs ?- 1
//                p.CompletedJobs <- pr.CompletedJobs ?+ 1
//                p)
//        |> Async.Ignore

    member this.SetRunning(pid : string) = 
        Table.transact2<ProcessRecord> config table pk pid 
           (fun pr -> 
                let p = pr.CloneDefault()
                if pr.Status.Value = int ProcessStatus.Posted then
                    p.Status <- nullable <| int ProcessStatus.Running
                    p.InitializationTime <- nullable DateTimeOffset.UtcNow
                p)
        |> Async.Ignore

    member this.SetCancelled(pid : string) = 
        Table.transact2<ProcessRecord> config table pk pid 
          (fun pr -> 
                let p = pr.CloneDefault()
                p.Status <- nullable <| int ProcessStatus.Canceled
                p.CompletionTime <- nullable DateTimeOffset.UtcNow
                p.Completed <- nullable true
                p)
        |> Async.Ignore

    member this.SetFaulted(pid : string) = 
        Table.transact2<ProcessRecord> config table pk pid 
          (fun pr -> 
                let p = pr.CloneDefault()
                p.Status <- nullable <| int ProcessStatus.Faulted
                p.CompletionTime <- nullable DateTimeOffset.UtcNow
                p.Completed <- nullable true
                p)
        |> Async.Ignore

    member this.SetCancellationRequested(pid : string) = 
        Table.transact2<ProcessRecord> config table pk pid 
          (fun pr -> 
                let p = pr.CloneDefault()
                match enum<ProcessStatus> pr.Status.Value with
                | ProcessStatus.Posted 
                | ProcessStatus.Running ->
                    p.Status <- nullable <| int ProcessStatus.CancellationRequested
                | _ -> ()
                p)
        |> Async.Ignore

    member this.SetCompleted(pid : string) =
        Table.transact2<ProcessRecord> config table pk pid 
          (fun pr -> 
                let p = pr.CloneDefault()
                p.Status <- nullable <| int ProcessStatus.Completed
                p.CompletionTime <- nullable DateTimeOffset.UtcNow
                p.Completed <- nullable true
                p)
        |> Async.Ignore

    member this.GetProcess(pid : string) = Table.read<ProcessRecord> config table pk pid

    member this.GetProcesses () = Table.queryPK<ProcessRecord> config table pk
    
    member this.ClearProcess (pid : string, full, force) = async {
        let! record = this.GetProcess(pid)
        if force = false && not record.Completed.Value then
            failwithf "Cannot clear process %s. Process not completed." pid 
        if record <> Unchecked.defaultof<_> then
            do! Table.delete<ProcessRecord> config table record
        if full then
            let! rks = Table.queryDynamic config table pid
            rks |> Array.iter(fun de -> de.ETag <- "*")
            do! Table.deleteBatch config table rks
            let bc = ConfigurationRegistry.Resolve<StoreClientProvider>(config).BlobClient.GetContainerReference(config.RuntimeContainer)
            let dir = bc.GetDirectoryReference(pid)
            let processBlobs = dir.ListBlobs()
            let refs = processBlobs
                        |> Seq.map (fun b -> dir.GetBlockBlobReference(b.Uri.Segments |> Seq.last))
            do! refs |> Seq.map (fun r -> r.DeleteIfExistsAsync())
                     |> Seq.map Async.AwaitTask
                     |> Async.Parallel
                     |> Async.Ignore
            ()
    }

    member this.ClearAllProcesses (force, full) = async {
        let! ps = this.GetProcesses()
        let xs = ResizeArray<exn>()
        for p in ps do 
            let! result = Async.Catch <| this.ClearProcess(p.Id, force, full)
            match result with
            | Choice2Of2 e -> xs.Add(e)
            | _ -> ()
        if xs.Count > 0 then return raise <| AggregateException(xs)
    }