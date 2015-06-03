﻿namespace MBrace.Azure.Client

#nowarn "52"

open System
open System.IO
open System.Threading
open System.Reflection
open System.Collections.Concurrent

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils.PrettyPrinters
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Info
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure.Runtime.Primitives
open Microsoft.FSharp.Linq

[<AutoSerializable(false); AbstractClass>]
/// Represents a cloud process.
type Process internal (config, pid : string, ty : Type, pmon : ProcessManager, jman : JobManager) = 
    
    let proc = 
        new Live<_>((fun () -> pmon.GetProcess(pid)), initial = Choice2Of2(exn ("Process not initialized")), 
                    keepLast = true, interval = 500)

    let jobs =
        new Live<_>((fun () -> jman.List(pid)), initial = Choice2Of2(exn ("Process not initialized")), keepLast = true, interval = 500)

    let logger = new ProcessLogger(config, pid)
    let dcts = lazy DistributedCancellationTokenSource.FromPath(config, proc.Value.CancellationPartitionKey, proc.Value.CancellationRowKey)

    member internal this.ProcessEntity = proc
    member internal this.DistributedCancellationTokenSource = dcts.Value
    
    /// Block until process record is updated. 
    /// Workaround because ResultCell.SetResult and Process.SetCompleted
    /// are not batched.
    member internal this.AwaitCompletionAsync () = 
        let rec loop () = async {
            if proc.Value.Completed.GetValueOrDefault() then 
                return () 
            else 
                do! Async.Sleep 100 
                return! loop ()
        }
        loop ()

    /// Awaits for the process result.
    abstract AwaitResultBoxed : unit -> obj
    /// Asynchronously waits for the process result.
    abstract AwaitResultBoxedAsync : unit -> Async<obj>

    /// Process state.
    member this.Status = enum<ProcessStatus> proc.Value.Status.Value

    /// Returns process' CancellationTokenSource.
    member this.CancellationTokenSource = dcts.Value :> ICloudCancellationTokenSource

    /// Process id.    
    member this.Id = pid

    /// Process name.
    member this.Name = proc.Value.Name

    /// Process type.
    member this.Type = ty

    /// Returns the initialization time for this process.
    member this.InitializationTime = let init = proc.Value.InitializationTime.Value in init.ToLocalTime()
    
    /// Returns the execution time for this process.
    member this.ExecutionTime = 
        let s = 
            if proc.Value.Completed.GetValueOrDefault() then proc.Value.CompletionTime.Value
            else DateTimeOffset.UtcNow
        s - proc.Value.InitializationTime.Value
    
    /// Returns iff the process is completed.
    member this.Completed : bool = proc.Value.Completed.Value

    /// Returns the number of tasks created by this process and are currently executing.
    member this.ActiveJobs : int = 
        jobs.Value |> Seq.filter (fun j -> j.Status = JobStatus.Active) |> Seq.length

    /// Returns the number of tasks created by this process.
    member this.TotalJobs : int =
        jobs.Value |> Seq.length

    /// Returns the number of tasks completed by this process.
    member this.CompletedJobs : int =
        jobs.Value |> Seq.filter (fun j -> j.Status = JobStatus.Completed) |> Seq.length

    /// Returns the number of tasks failed to execute by this process.
    //member this.FaultedJobs : int = proc.Value.FaultedJobs.Value

    /// Sends a kill signal for this process.
    member this.Kill() = Async.RunSync(this.KillAsync())
    /// Asynchronously sends a kill signal for this process.
    member this.KillAsync() = async {
            do! pmon.SetCancellationRequested(pid)
            do this.DistributedCancellationTokenSource.Cancel()
        }

    /// Asynchronously returns all cloud logs for this process.
    member this.GetLogsAsync(?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) = 
        logger.GetLogs(?fromDate = fromDate, ?toDate = toDate)
    /// Returns all cloud logs for this process.
    member this.GetLogs(?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) =
        Async.RunSync(this.GetLogsAsync(?fromDate = fromDate, ?toDate = toDate))

    /// Prints all cloud logs for this process.
    member this.ShowLogs(?fromDate : DateTimeOffset, ?toDate : DateTimeOffset) = 
        printf "%s" <| LogReporter.Report(this.GetLogs(?fromDate = fromDate, ?toDate = toDate), sprintf "Process %s logs" pid, false)

    /// Prints a detailed report for this process.
    member this.ShowInfo () = printf "%s" <| ProcessReporter.Report([this], "Process", false)

    member this.GetJobs() = jobs.Value //Async.RunSync(this.GetJobsAsync())

    member this.ShowJobs() = printfn "%s" <| JobReporter.Report(this.GetJobs(), sprintf "Jobs for process %A" pid)

    //member this.GetJobsAsync() = jobs.Value //jman.List(pid)
    //member this.ShowJobsTree () = printfn "%s" <| JobReporter.ReportTreeView(this.GetJobs(), sprintf "Jobs for process %s" pid)
     
and internal ProcessReporter() = 
    static let optionToString (value : Option<'T>) = 
        match value with 
        | Some value -> value.ToString() 
        | None -> "N/A" 

    static let template : Field<ProcessRecord * seq<Job>> list = 
        [ Field.create "Name" Left (fun (p,_) -> p.Name)
          Field.create "Process Id" Right (fun (p,_) -> p.Id)
          Field.create "Status" Right (fun (p,_) -> enum<ProcessStatus> p.Status.Value)
          Field.create "Completed" Left (fun (p,_) -> p.Completed)
          Field.create "Execution Time" Left (fun (p,_) -> if p.Completed.GetValueOrDefault() then p.CompletionTime ?-? p.InitializationTime else DateTimeOffset.UtcNow -? p.InitializationTime)
          Field.create "Jobs" Center (fun (_,jobs) -> 
            let total = Seq.length jobs
            let count s = jobs |> Seq.filter (fun j -> j.Status = s) |> Seq.length
            sprintf "%3d / %3d / %3d" (count JobStatus.Active) (count JobStatus.Completed) total)
          Field.create "Result Type" Left (fun (p,_) -> p.TypeName) 
          Field.create "Initialization time" Left (fun (p,_) -> p.InitializationTime.ToOption() |> optionToString)
          Field.create "Completion time" Left (fun (p,_) -> p.CompletionTime.ToOption() |> optionToString)
        ]

    /// No need to have a Process obj (having dependencies downloaded, etc)    
    static member Report(processes : seq<ProcessRecord * seq<Job>>, title, borders) =
        let ps = processes 
                 |> Seq.sortBy (fun (p,_) -> p.InitializationTime.GetValueOrDefault())
                 |> Seq.toList
        sprintf "%s\nJobs : Active / Completed / Total\n" <| Record.PrettyPrint(template, ps, title, borders)

    static member Report(processes : seq<Process>, title, borders) = 
        let ps = processes |> Seq.map (fun p -> p.ProcessEntity.Value, p.GetJobs()) |> Seq.toArray
        ProcessReporter.Report(ps, title, borders)



[<AutoSerializable(false)>]
/// Represents a cloud process.
type Process<'T> internal (config, pid : string, pmon : ProcessManager, jman : JobManager) = 
    inherit Process(config, pid, typeof<'T>, pmon, jman) 

    override this.AwaitResultBoxed () : obj = this.AwaitResultBoxedAsync() |> Async.RunSync 
    override this.AwaitResultBoxedAsync () : Async<obj> =
        async {
            let rc : ResultCell<'T> = ResultCell.FromPath(config, this.ProcessEntity.Value.Id, this.ProcessEntity.Value.ResultRowKey)
            let! r = rc.AwaitResult()
            do! this.AwaitCompletionAsync()
            return r.Value :> obj
        }

    /// Awaits for the process result.
    member this.AwaitResult() : 'T = this.AwaitResultAsync() |> Async.RunSync
    /// Asynchronously waits for the process result.
    member this.AwaitResultAsync() : Async<'T> = 
        async {
            let rc : ResultCell<'T> = ResultCell.FromPath(config, this.ProcessEntity.Value.Id, this.ProcessEntity.Value.ResultRowKey)
            let! r = rc.AwaitResult()
            do! this.AwaitCompletionAsync()
            return r.Value
        }

    static member internal Create(config : ConfigurationId, pid : string, ty : Type, pmon : ProcessManager, jman : JobManager) : Process =
        let processT = typeof<Process<_>>.GetGenericTypeDefinition().MakeGenericType [| ty |]
        let flags = BindingFlags.NonPublic ||| BindingFlags.Instance
        let culture = System.Globalization.CultureInfo.InvariantCulture
        Activator.CreateInstance(processT, flags, null, [|config :> obj; pid :> obj ; pmon :> obj; jman :> obj |], culture) :?> Process


[<AutoSerializable(false); Sealed>]
type internal ProcessCache () =
    static let cache = new ConcurrentDictionary<string, Process>()

    static member Add(ps : Process) = cache.TryAdd(ps.Id, ps) |> ignore
    static member TryGet(pid : string) = 
        match cache.TryGetValue(pid) with
        | true, p -> Some p
        | false, _ -> None