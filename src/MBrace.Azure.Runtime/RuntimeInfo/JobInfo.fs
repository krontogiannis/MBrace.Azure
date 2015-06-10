namespace MBrace.Azure.Runtime.Info

open MBrace.Core.Internals
open MBrace.Azure
open System
open Microsoft.WindowsAzure.Storage.Table
open MBrace.Azure.Runtime.Utilities
open System.Collections.Generic

[<AllowNullLiteral>]
type JobRecord (processId, jobId) =
    inherit TableEntity(processId, jobId)
    member val Id : string = jobId with get, set
    member val ParentId : string = null with get, set
    member val ProcessId : string = null with get, set
    
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
    member val CreationTime = Nullable<DateTimeOffset>() with get, set
    member val StartTime = Nullable<DateTimeOffset>() with get, set
    member val CompletionTime = Nullable<DateTimeOffset>() with get, set

    new () = JobRecord(null, null)


[<AutoOpen>]
module JobCache =
    type private JobId = string
    type private ProcessId = string
    type private Factory = Async<JobRecord []>
    type private CacheMessage =
        | AddFactory of pid : ProcessId * factory : Factory
        | GetRecords of pid : ProcessId * AsyncReplyChannel<JobRecord []>
        | GetRecord  of pid : ProcessId * jobId : JobId * AsyncReplyChannel<JobRecord>

    [<AutoSerializable(false); Sealed; AbstractClass>]
    type JobCache private () =
        static let cacheAgent =
            let cache = new Dictionary<ProcessId, Dictionary<JobId, JobRecord>>()
            let factories = new Dictionary<ProcessId, Factory>()
            let timestamps = new Dictionary<ProcessId, DateTime>()
            let updateInterval = 500
            let factoryFilter = 
                let factoryInterval = TimeSpan.FromMinutes(2.)
                fun pid -> DateTime.Now - timestamps.[pid] <= factoryInterval

            let agent = 
                MailboxProcessor<CacheMessage>.Start(fun inbox ->
                    let rec loop () = async {
                        let! msg = inbox.TryReceive(updateInterval)
                        match msg with
                        | None ->
                            let! records = factories
                                           |> Seq.filter(fun kvp -> factoryFilter kvp.Key)
                                           |> Seq.map(fun kvp -> kvp.Value)
                                           |> Async.Parallel
                                           |> Async.Catch

                            match records with
                            | Choice1Of2 records ->
                                records 
                                |> Seq.collect id 
                                |> Seq.iter (fun r -> 
                                    match cache.TryGetValue(r.ProcessId) with
                                    | true, c -> c.[r.Id] <- r
                                    | false, _ -> 
                                        let c = new Dictionary<JobId, JobRecord>()
                                        c.[r.Id] <- r
                                        cache.[r.ProcessId] <- c)
                            | Choice2Of2 _ -> ()
                        | Some(AddFactory(pid, factory)) ->
                            timestamps.[pid] <- DateTime.Now
                            factories.[pid] <- factory
                        | Some(GetRecords(pid, ch)) ->
                            let! records = async {
                                match cache.TryGetValue(pid) with
                                | true, jobs when factoryFilter pid ->
                                    return jobs |> Seq.map (fun kvp -> kvp.Value) |> Seq.toArray
                                | _ ->
                                    return! factories.[pid]
                                }
                            timestamps.[pid] <- DateTime.Now
                            ch.Reply(records)
                        | Some(GetRecord(pid, jobId, ch)) ->
                            let! record = async {
                                match cache.TryGetValue(pid) with
                                | true, jobs when factoryFilter pid && jobs.ContainsKey(jobId) ->
                                    return cache.[pid].[jobId]
                                | true, _ ->
                                    let! jobs = factories.[pid]
                                    match jobs |> Seq.tryFind (fun r -> r.Id = jobId) with
                                    | Some r -> return r
                                    | None -> return null
                                | false, _ ->
                                    return null
                            }
                            timestamps.[pid] <- DateTime.Now
                            ch.Reply(record)
                        return! loop ()
                    }
                    loop ())

            agent.Error.Add(fun e -> Console.WriteLine("JobCache unexpected error {0}", e))
            agent

        static member AddJobFactory(pid : string, factory : Factory) : unit =
            cacheAgent.Post(AddFactory(pid, factory))

        static member GetRecords(pid : string) : JobRecord [] =
            cacheAgent.PostAndReply(fun ch -> GetRecords(pid, ch))

        static member GetRecord(pid : string, jobId : string) : JobRecord =
            cacheAgent.PostAndReply(fun ch -> GetRecord(pid, jobId, ch))

namespace MBrace.Azure

open MBrace.Core.Internals
open MBrace.Runtime.Utils
open MBrace.Azure.Runtime.Primitives
open MBrace.Azure.Runtime.Info
open MBrace.Azure.Runtime.Utilities
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
    | Posting   = 0
    | Posted    = 1
    | Active    = 2
    | Inactive  = 3
    | Completed = 4

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
            failwithf "Invalid JobRecord %s" jobRecord.Id
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
        
/// Represents a unit of work that can be executed by the distributed runtime.
type Job internal (config : ConfigurationId, job : JobRecord) =
    let getJob () =
        JobCache.GetRecord(job.ProcessId, job.Id)

    /// Job unique identifier.
    member this.Id = getJob().Id
    /// Job's PID.
    member this.ProcessId = getJob().ProcessId
    /// Parent job identifier.
    member this.ParentId = getJob().ParentId
    /// Type of this job.
    member this.JobType = parseJobType <| getJob()
    /// Job status.
    member this.Status = parseJobStatus <| getJob()
    /// Worker executing this job.
    member this.WorkerId = getJob().WorkerId
    /// Job return type.
    member this.ReturnType = getJob().ReturnType
    /// Job timestamp, used for active jobs.
    member this.Timestamp = getJob().Timestamp
    /// The number of times this job has been dequeued for execution.
    member this.DeliveryCount = getJob().DeliveryCount.Value
    /// The point in time this job was posted.
    member this.CreationTime = getJob().CreationTime.Value
    /// The point in time this job was marked as Active.
    member this.StartTime = getJob().StartTime.ToOption()
    /// The point in time this job completed.
    member this.CompletionTime = getJob().CompletionTime.ToOption()
    /// Approximation of the job's serialized size in bytes.
    member this.JobSize = getJob().Size.GetValueOrDefault()

    member internal this.ResultPartition = getJob().ResultPartition
    member internal this.ResultRow = getJob().ResultRow
    member internal this.ConfigurationId = config

    override this.GetHashCode() = hash this.Id

    override this.Equals(obj : obj) =
        match obj with
        | :? Job as j -> this.Id = j.Id
        | _ -> false

namespace MBrace.Azure.Runtime.Info

open System
open MBrace.Azure
open MBrace.Runtime.Utils
open MBrace.Core.Internals
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure.Runtime.Primitives
open System.Text

[<AutoSerializableAttribute(false)>]
type JobManager private (config : ConfigurationId, logger : ICloudLogger) =
    static let mkPartitionKey pid = sprintf "job:%s" pid
    
    static let heartBeatInterval = 30000

    static let mkRecord pid jobId jobType returnType parentId size resPk resRk =
        let job = new JobRecord(mkPartitionKey pid, jobId)
        assignJobStatus job JobStatus.Posting
        assignJobType job jobType
        job.ProcessId <- pid
        job.CreationTime <- nullable DateTimeOffset.UtcNow
        job.ReturnType <- returnType
        job.ParentId <- parentId
        job.Size <- nullable size
        job.ResultPartition <- resPk
        job.ResultRow <- resRk
        job.DeliveryCount <- nullable 0
        job

    let fetchAll pid =
        Table.queryPK<JobRecord> config config.RuntimeTable (mkPartitionKey pid)

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

    member this.UpdateBatch(pid, jobIds : string seq, status : JobStatus) =
        async {
            let jobs = jobIds 
                        |> Seq.map (fun jobId -> new JobRecord(mkPartitionKey pid, jobId, ETag = "*", Status = nullable(int status)))
            do! Table.mergeBatch config config.RuntimeTable jobs
        }

    member this.Update(pid, jobId, status, ?workerId, ?deliveryCount) =
        async {
            let job = new JobRecord(mkPartitionKey pid, jobId)
            
            assignJobStatus job status
            
            match status with
            | JobStatus.Posted    -> job.CreationTime <- nullable DateTimeOffset.UtcNow
            | JobStatus.Active    -> job.StartTime <- nullable DateTimeOffset.UtcNow
            | JobStatus.Completed -> job.CompletionTime <- nullable DateTimeOffset.UtcNow
            | _                   -> failwithf "Invalid status %A" status

            deliveryCount |> Option.iter (fun dc -> job.DeliveryCount <- nullable dc)
            workerId |> Option.iter (fun wid -> job.WorkerId <- wid)
            job.ETag <- "*"
            let! _job = Table.merge config config.RuntimeTable job
            return ()
        }

    member this.Fetch(pid : string) =
        async {
            let! jobs = fetchAll pid
            return jobs |> Seq.map (fun job -> new Job(config, job))
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

    member this.Fetch(pid : string, jobId : string) =
        async {
            let! job = Table.read<JobRecord> config config.RuntimeTable (mkPartitionKey pid) jobId
            if job <> null then
                return new Job(config, job)
            else
                return failwithf "Job %A not found" jobId
        }

    member this.AddToCache(pid) =
        JobCache.AddJobFactory(pid, fetchAll pid)

    member this.GetCached(pid : string) =
        JobCache.GetRecords(pid)
        |> Seq.map (fun r -> new Job(config, r))

    member this.GetCached(pid : string, jobId : string) =
        new Job(config, JobCache.GetRecord(pid, jobId))

    static member Create (config : ConfigurationId, logger) = new JobManager(config, logger)

    // Non-'public' APIs that might come useful for debugging.

    /// Try get job's partial result. Experimental.
    static member TryGetResultAsync<'T>(job : Job) = 
        async {
                match job.JobType with
                | Root | Task | TaskAffined _ -> 
                    let! result = ResultCell<'T>.FromPath(job.ConfigurationId, job.ResultPartition, job.ResultRow).TryGetResult()
                    match result with
                    | Some r -> return Some r.Value
                    | None -> return None
                | Parallel(i,m) | ParallelAffined(_,i,m) ->
                    return! ResultAggregator.Get(job.ConfigurationId, job.ResultPartition, job.ResultRow, m+1).TryGetResult(i)
                | Choice _ | ChoiceAffined _ ->
                    return! Async.Raise(NotSupportedException("Partial result not supported for Choice."))
        }
        
    /// Show jobs as a tree view.
    static member ReportTreeView(jobs : Job seq) =
        let sb = new StringBuilder()

        let append (job : Job) =
            let sb = sb.AppendFormat("{0}...{1} ",job.Id.Substring(0,7), job.Id.Substring(job.Id.Length - 3))
                       .AppendFormat("{0} {1} {2} ", job.JobType, getHumanReadableByteSize job.JobSize, job.Status)
            let sb = 
                match job.CompletionTime with 
                | Some t -> sb.Append(t - job.StartTime.Value)
                | None -> sb
            sb.AppendLine()

        let root = jobs |> Seq.find (fun j -> j.JobType = Root)

        let child (current : Job) =
            jobs |> Seq.filter (fun j -> j.ParentId = current.Id)

        let rec treeview (current : Job) depth : unit =
            if depth > 0 then
                for i = 0 to 4 * (depth-1) - 1 do 
                    ignore <| sb.Append(if i % 4 = 0 then '|' else ' ')
                let _ = sb.Append("├───") // fancy
                ()
            let _ = append current
            child current |> Seq.iter (fun j -> treeview j (depth + 1))
        treeview root 0
        Console.WriteLine(sb)

