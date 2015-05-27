namespace MBrace.Azure.Runtime.Info

open MBrace.Core.Internals

open MBrace.Azure

open System
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Azure.Runtime.Utilities

type JobRecord (processId, jobId) =
    inherit TableEntity(processId, jobId)
    member val Id : string = jobId with get, set
    member val ParentId : string = null with get, set
    
    member val Kind = Nullable<int>() with get, set
    member val Affinity : string = null with get, set
    member val Index = Nullable<int>() with get, set
    member val MaxIndex = Nullable<int>() with get, set

    member val Status = Nullable<int>() with get, set
    member val ResultPartition : string = null with get, set
    member val ResultRow : string = null with get, set
    member val ReturnType : string = null with get, set
    member val WorkerId : string = null with get, set
    member val Size = Nullable<int64>() with get, set
    member val DeliveryCount = Nullable<int>() with get, set
    member val InitializationTime = Nullable<DateTimeOffset>() with get, set
    member val CompletionTime = Nullable<DateTimeOffset>() with get, set

    new () = JobRecord(null, null)

namespace MBrace.Azure

open MBrace.Core.Internals
open MBrace.Runtime.Utils
open MBrace.Azure.Runtime.Primitives
open MBrace.Azure.Runtime.Info
open System

/// Job kind.
type JobType =
    /// Root job for process.
    | Root
    /// Job created by Cloud.StartAsTask
    | Task
    /// Job created by Cloud.StartAsTask with affinity.
    | TaskAffined of affinity : string
    /// Job created by Cloud.Parallel.
    | Parallel of index : int * maxIndex : int
    /// Job created by Cloud.Choice.
    | Choice of index : int * maxIndex : int
    /// Job created by Cloud.Parallel with affinity.
    | ParallelAffined of affinity : string * index : int * maxIndex : int
    /// Job created by Cloud.Choice with affinity.
    | ChoiceAffined of affinity : string * index : int * maxIndex : int

    override this.ToString() =
        match this with
        | TaskAffined a -> sprintf "Task(%s)" a
        | ParallelAffined(a,i,m) -> sprintf "Parallel(%s,%d,%d)" a i m 
        | ChoiceAffined(a,i,m) -> sprintf "Choice(%s,%d,%d)" a i m 
        | _ -> sprintf "%A" this

/// Job status
type JobStatus =
    | Posted    = 0
    | Active    = 1
    | Inactive  = 2
    | Completed = 3
    | Cancelled = 4
    | Suspended = 5

// Stored in table.
type private JobKind =
    | Root            = 0
    | Task            = 1
    | TaskAffined     = 2
    | Parallel        = 3
    | Choice          = 4
    | ParallelAffined = 5
    | ChoiceAffined   = 6

[<AutoOpen>]
module private Helpers =
    open MBrace.Azure.Runtime.Utilities
    open MBrace.Azure.Runtime.Info

    let parseJobType (jobRecord : JobRecord) =
        let invalid () =
            failwithf "Invalid JobRecord %+A" jobRecord
        let getIdx () =
            match jobRecord.Index.HasValue, jobRecord.MaxIndex.HasValue with
            | true, true -> jobRecord.Index.Value, jobRecord.MaxIndex.Value
            | _ -> invalid()
        if not jobRecord.Kind.HasValue then
            invalid ()
        else
            let kind = enum<JobKind>(jobRecord.Kind.Value)
            match kind with
            | JobKind.Root -> Root
            | JobKind.Task -> Task
            | JobKind.TaskAffined when jobRecord.Affinity <> null -> TaskAffined(jobRecord.Affinity)
            | JobKind.Parallel -> let idx, maxIdx = getIdx() in Parallel(idx, maxIdx)
            | JobKind.Choice -> let idx, maxIdx = getIdx() in Choice(idx, maxIdx)
            | JobKind.ParallelAffined when jobRecord.Affinity <> null ->
                let idx, maxIdx = getIdx()
                ParallelAffined(jobRecord.Affinity, idx, maxIdx)
            | JobKind.ChoiceAffined when jobRecord.Affinity <> null ->
                let idx, maxIdx = getIdx()
                ChoiceAffined(jobRecord.Affinity, idx, maxIdx)
            | _ -> invalid ()

    let assignJobType (jobRecord : JobRecord) (jobType : JobType) =
        match jobType with
        | Root -> 
            jobRecord.Kind <- nullable(int JobKind.Root)
        | Task -> 
            jobRecord.Kind <- nullable(int JobKind.Task)
        | TaskAffined a -> 
            jobRecord.Kind <- nullable(int JobKind.TaskAffined)
            jobRecord.Affinity <- a
        | Parallel(i,m) ->
            jobRecord.Kind <- nullable(int JobKind.Parallel)
            jobRecord.Index <- nullable i
            jobRecord.MaxIndex <- nullable m
        | Choice(i,m) ->
            jobRecord.Kind <- nullable(int JobKind.Choice)
            jobRecord.Index <- nullable i
            jobRecord.MaxIndex <- nullable m
        | ParallelAffined(a,i,m) ->
            jobRecord.Kind <- nullable(int JobKind.ParallelAffined)
            jobRecord.Affinity <- a
            jobRecord.Index <- nullable i
            jobRecord.MaxIndex <- nullable m
        | ChoiceAffined(a,i,m) ->
            jobRecord.Kind <-nullable(int JobKind.ChoiceAffined)
            jobRecord.Affinity <- a
            jobRecord.Index <- nullable i
            jobRecord.MaxIndex <- nullable m

    let parseJobStatus (jobRecord : JobRecord) =
        if jobRecord.Status.HasValue then
            enum<JobStatus>(jobRecord.Status.Value)
        else
            failwith "Invalid Job Status %+A" jobRecord

    let assignJobStatus (jobRecord : JobRecord) (status : JobStatus) =
        jobRecord.Status <- nullable(int status)
        
type JobInfo internal (config : ConfigurationId, job : JobRecord) =
    let jobType = parseJobType job
    let status = parseJobStatus job

    member this.Id = job.Id
    member this.ParentId = job.ParentId
    member this.JobType = jobType
    member this.Status = status
    member this.WorkerId = job.WorkerId
    member this.ReturnType = job.ReturnType
    member this.Timestamp = job.Timestamp
    member this.DeliveryCount = job.DeliveryCount.GetValueOrDefault(-1)
    member this.InitializationTime = job.InitializationTime
    member this.CompletionTime = job.CompletionTime
    member this.JobSize = job.Size.GetValueOrDefault()

    member this.TryGetResult<'T>() = Async.RunSync(this.TryGetResultAsync<'T>())

    member this.TryGetResultAsync<'T>() = 
        match this.JobType with
        | Root | Task | TaskAffined _ -> 
            ResultCell<'T>.FromPath(config, job.ResultPartition, job.ResultRow).TryGetResult()
        | Parallel(i,m) | ParallelAffined(_,i,m) ->
            ResultAggregator.Get(config, job.ResultPartition, job.ResultRow, m+1).TryGetResult(i)
        | Choice _ | ChoiceAffined _ ->
            raise(NotSupportedException("Partial result not supported for Choice."))

namespace MBrace.Azure.Runtime.Info

open System
open MBrace.Azure
open MBrace.Runtime.Utils
open MBrace.Core.Internals
open MBrace.Azure.Runtime.Utilities


[<AutoSerializableAttribute(false)>]
type JobManager private (config : ConfigurationId, logger : ICloudLogger) =
    static let mkPartitionKey pid = sprintf "jobInfo:%s" pid

    static let heartBeatInterval = 30000

    static let mkRecord pid jobId jobType returnType parentId size resPk resRk =
        let job = new JobRecord(mkPartitionKey pid, jobId)
        assignJobStatus job JobStatus.Posted
        assignJobType job jobType
        job.ReturnType <- returnType
        job.ParentId <- parentId
        job.Size <- nullable size
        job.ResultPartition <- resPk
        job.ResultRow <- resRk
        job

    member this.Create(pid, jobId, jobType : JobType, returnType, parentId, size : int64, resPk, resRk) =
        async {
            let job = mkRecord pid jobId jobType returnType parentId size resPk resRk
            do! Table.insert config config.RuntimeTable job 
        }

    member this.CreateBatch(pid, info : (string * JobType * int64) seq, returnType, parentId, resPk, resRk) =
        async {
            let jobs = 
                info 
                |> Seq.map (fun (jobId, jobType, size) ->
                    mkRecord pid jobId jobType returnType parentId size resPk resRk)
            do! Table.insertBatch config config.RuntimeTable jobs 
        }

    member this.Update(pid, jobId, status, ?workerId, ?deliveryCount) =
        async {
            let job = new JobRecord(mkPartitionKey pid, jobId)
            
            assignJobStatus job status
            
            match status with
            | JobStatus.Posted | JobStatus.Active -> 
                job.InitializationTime <- nullable DateTimeOffset.UtcNow
            | JobStatus.Completed | JobStatus.Cancelled -> 
                job.CompletionTime <- nullable DateTimeOffset.UtcNow
            | _ -> ()

            deliveryCount |> Option.iter (fun dc -> job.DeliveryCount <- nullable dc)
            workerId |> Option.iter (fun wid -> job.WorkerId <- wid)
            job.ETag <- "*"
            let! _job = Table.merge config config.RuntimeTable job
            return ()
        }

    member this.List(pid : string) =
        async {
            let! jobs = Table.queryPK<JobRecord> config config.RuntimeTable (mkPartitionKey pid)
            return jobs |> Seq.map (fun job -> new JobInfo(config, job))
        }

    member this.Heartbeat(pid, jobId) =
        async {
            let job = new JobRecord(mkPartitionKey pid, jobId, ETag = "*")
            let! j = Table.merge config config.RuntimeTable job
            let job' = ref j
            while job'.Value.Status.Value = int JobStatus.Active do
                do! Async.Sleep heartBeatInterval
                let! j = Async.Catch <| Table.merge config config.RuntimeTable job
                match j with
                | Choice1Of2 j -> job' := j     
                | Choice2Of2 e -> logger.Logf "Failed to give heartbeat for Job %s : %+A" jobId e
            logger.Logf "Stopped heartbeat loop for Job %s" jobId
        } |> Async.StartChild

    static member Create (config : ConfigurationId, logger) = new JobManager(config, logger)