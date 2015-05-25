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

namespace MBrace.Azure

open MBrace.Azure.Runtime.Info

/// Job kind.
type JobType =
    /// Root job for process.
    | Root
    /// Job created by Cloud.StartChild.
    | StartChild
    /// Job created by Cloud.StartChild with affinity.
    | Affined of affinity : string
    /// Job created by Cloud.Parallel.
    | Parallel of index : int * maxIndex : int
    /// Job created by Cloud.Choice.
    | Choice of index : int * maxIndex : int
    /// Job created by Cloud.Parallel with affinity.
    | ParallelAffined of affinity : string * index : int * maxIndex : int
    /// Job created by Cloud.Choice with affinity.
    | ChoiceAffined of affinity : string * index : int * maxIndex : int

/// Job status
type JobStatus =
    | Enqueued
    | Active
    | Inactive
    | Completed

[<AutoOpen>]
module private Helpers =
    open MBrace.Azure.Runtime.Utilities

    [<Literal>]
    let RootL            = "Root"
    [<Literal>]
    let StartChildL      = "StartChild"
    [<Literal>]
    let AffinedL         = "Affined"
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
        | StartChildL -> StartChild
        | AffinedL when jobRecord.Affinity <> null -> Affined(jobRecord.Affinity)
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
        | StartChild -> 
            jobRecord.JobType <- StartChildL
        | Affined a -> 
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
        | "Enqueued" -> Enqueued
        | "Active" -> Active
        | "Inactive" -> Inactive
        | "Completed" -> Completed
        | _ -> failwith "Invalid Job Status %+A" jobRecord

    let assignJobStatus (jobRecord : JobRecord) (status : JobStatus) =
        match status with
        | Enqueued -> jobRecord.Status <- "Enqueued"
        | Active -> jobRecord.Status <- "Active"
        | Inactive -> jobRecord.Status <- "Inactive"
        | Completed -> jobRecord.Status <- "Completed"
        
type JobInfo internal (jobRecord : JobRecord) =
    let jobType = parseJobType jobRecord
    let status = parseJobStatus jobRecord

    member this.Id = jobRecord.Id
    member this.ParentId = jobRecord.ParentId
    member this.JobType = jobType
    member this.Status = status
    
    // TODO : Add CTS and partial result

type internal JobInfoManager (config : ConfigurationId) =
    class
    end