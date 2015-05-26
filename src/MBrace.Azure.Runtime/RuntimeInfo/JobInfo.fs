namespace MBrace.Azure.Runtime.Info

open System
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Azure.Runtime.Utilities

type JobRecord (processId, jobId) =
    inherit TableEntity(processId, jobId)
    member val ProcessId : string = processId with get, set
    member val Id : string = jobId with get, set
    member val ParentId : string = null with get, set
    member val JobType : string = null with get, set
    
    member val Affinity : string = null with get, set
    member val Index = Nullable<int>() with get, set
    member val MaxIndex = Nullable<int>() with get, set

    member val Status : string = null with get, set
    member val CancellationUri : string = null with get, set
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

open MBrace.Azure.Runtime.Utilities
open MBrace.Azure.Runtime.Info
open MBrace.Core.Internals
open MBrace.Runtime.Utils
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
        | Root -> "Root"
        | Task -> "Task"
        | TaskAffined a -> sprintf "Task(%s)" a
        | Parallel(i,m) -> sprintf "Parallel(%d,%d)" i m 
        | Choice(i,m) -> sprintf "Choice(%d,%d)" i m 
        | ParallelAffined(a,i,m) -> sprintf "Parallel(%s,%d,%d)" a i m 
        | ChoiceAffined(a,i,m) -> sprintf "Choice(%s,%d,%d)" a i m 

/// Job status
type JobStatus =
    | Posted
    | Active
    | Inactive
    | Completed

[<AutoOpen>]
module private Helpers =
    open MBrace.Azure.Runtime.Utilities

    [<Literal>]
    let RootL            = "Root"
    [<Literal>]
    let TaskL            = "Task"
    [<Literal>]
    let AffinedL         = "TaskAffined"
    [<Literal>] 
    let ParallelL        = "Parallel"
    [<Literal>] 
    let ChoiceL          = "Choice"
    [<Literal>] 
    let ParallelAffinedL = "ParallelAffined"
    [<Literal>] 
    let ChoiceAffinedL   = "ChoiceAffined"

    let parseJobType (jobRecord : JobRecord) =
        let invalid () =
            failwithf "Invalid JobRecord %+A" jobRecord
        let getIdx () =
            match jobRecord.Index.HasValue, jobRecord.MaxIndex.HasValue with
            | true, true -> jobRecord.Index.Value, jobRecord.MaxIndex.Value
            | _ -> invalid()
        match jobRecord.JobType with
        | RootL -> Root
        | TaskL -> Task
        | AffinedL when jobRecord.Affinity <> null -> TaskAffined(jobRecord.Affinity)
        | ParallelL -> let idx, maxIdx = getIdx() in Parallel(idx, maxIdx)
        | ChoiceL -> let idx, maxIdx = getIdx() in Choice(idx, maxIdx)
        | ParallelAffinedL when jobRecord.Affinity <> null ->
            let idx, maxIdx = getIdx()
            ParallelAffined(jobRecord.Affinity, idx, maxIdx)
        | ChoiceAffinedL when jobRecord.Affinity <> null ->
            let idx, maxIdx = getIdx()
            ChoiceAffined(jobRecord.Affinity, idx, maxIdx)
        | _ -> invalid ()

    let assignJobType (jobRecord : JobRecord) (jobType : JobType) =
        match jobType with
        | Root -> 
            jobRecord.JobType <- RootL
        | Task -> 
            jobRecord.JobType <- TaskL
        | TaskAffined a -> 
            jobRecord.JobType <- AffinedL
            jobRecord.Affinity <- a
        | Parallel(i,m) ->
            jobRecord.JobType <- ParallelL
            jobRecord.Index <- nullable i
            jobRecord.MaxIndex <- nullable m
        | Choice(i,m) ->
            jobRecord.JobType <- ChoiceL
            jobRecord.Index <- nullable i
            jobRecord.MaxIndex <- nullable m
        | ParallelAffined(a,i,m) ->
            jobRecord.JobType <- ParallelAffinedL
            jobRecord.Affinity <- a
            jobRecord.Index <- nullable i
            jobRecord.MaxIndex <- nullable m
        | ChoiceAffined(a,i,m) ->
            jobRecord.JobType <- ChoiceAffinedL
            jobRecord.Affinity <- a
            jobRecord.Index <- nullable i
            jobRecord.MaxIndex <- nullable m

    let parseJobStatus (jobRecord : JobRecord) =
        match jobRecord.Status with
        | "Posted" -> Posted
        | "Active" -> Active
        | "Inactive" -> Inactive
        | "Completed" -> Completed
        | _ -> failwith "Invalid Job Status %+A" jobRecord

    let assignJobStatus (jobRecord : JobRecord) (status : JobStatus) =
        match status with
        | Posted -> jobRecord.Status <- "Posted"
        | Active -> jobRecord.Status <- "Active"
        | Inactive -> jobRecord.Status <- "Inactive"
        | Completed -> jobRecord.Status <- "Completed"
        
type JobInfo internal (job : JobRecord) =
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

    // TODO : implement properly
    member this.CancellationToken = job.CancellationUri
    member this.Result = sprintf "%s/%s" job.ResultPartition job.ResultRow

[<AutoSerializableAttribute(false)>]
type JobManager private (config : ConfigurationId, logger : ICloudLogger) =
    let mkPartitionKey pid = sprintf "jobInfo:%s" pid

    let heartBeatInterval = 30000

    let mkRecord pid jobId jobType returnType parentId size =
        let job = new JobRecord(mkPartitionKey pid, jobId)
        assignJobStatus job Posted
        assignJobType job jobType
        job.ReturnType <- returnType
        job.ParentId <- parentId
        job.Size <- nullable size
        job

    member this.Create(pid : string, jobId : string, jobType : JobType, returnType : string, parentId : string, size : int64) =
        async {
            let job = mkRecord pid jobId jobType returnType parentId size
            do! Table.insert config config.RuntimeTable job
        }

    member this.CreateBatch(pid, info : (string * JobType * int64) seq, returnType : string, parentId : string) =
        async {
            let jobs = 
                info 
                |> Seq.map (fun (jobId, jobType, size) ->
                    mkRecord pid jobId jobType returnType parentId size)
            do! Table.insertBatch config config.RuntimeTable jobs
        }

    member this.Update(pid, jobId, status, workerId, ?deliveryCount) =
        async {
            let job = new JobRecord(mkPartitionKey pid, jobId)
            
            assignJobStatus job status
            
            match status with
            | Posted | Active -> job.InitializationTime <- nullable DateTimeOffset.UtcNow
            | Completed       -> job.CompletionTime <- nullable DateTimeOffset.UtcNow
            | Inactive        -> ()

            deliveryCount 
            |> Option.iter (fun dc -> job.DeliveryCount <- nullable dc)

            job.WorkerId <- workerId
            job.ETag <- "*"
            let! _job = Table.merge config config.RuntimeTable job
            return ()
        }

    member this.List(pid : string) =
        async {
            let! jobs = Table.queryPK<JobRecord> config config.RuntimeTable (mkPartitionKey pid)
            return jobs |> Seq.map (fun job -> new JobInfo(job))
        }

    member this.Heartbeat(pid, jobId) =
        async {
            let job = new JobRecord(mkPartitionKey pid, jobId, ETag = "*")
            let! j = Table.merge config config.RuntimeTable job
            let job' = ref j
            while job'.Value.Status = "Active" do
                do! Async.Sleep heartBeatInterval
                let! j = Async.Catch <| Table.merge config config.RuntimeTable job
                match j with
                | Choice1Of2 j -> job' := j     
                | Choice2Of2 e -> logger.Logf "Failed to give heartbeat for Job %s : %+A" jobId e
            logger.Logf "Stopped heartbeat loop for Job %s" jobId
        } |> Async.StartChild

    static member Create (config : ConfigurationId, logger) = new JobManager(config, logger)