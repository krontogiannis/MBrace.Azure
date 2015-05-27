﻿namespace MBrace.Azure.Runtime.Primitives

open System
open System.Runtime.Serialization
open System.Threading

open Microsoft.WindowsAzure.Storage.Table

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities

/// Cloud computation result
type Result<'T> =
    | Completed of 'T
    | Exception of ExceptionDispatchInfo
    | Cancelled of OperationCanceledException
with
    member inline r.Value =
        match r with
        | Completed t -> t
        | Exception edi -> ExceptionDispatchInfo.raise true edi
        | Cancelled c -> ExceptionDispatchInfo.raiseWithCurrentStackTrace true c

/// Used to bypass typed Result<'T> in ResultCells in case of FaultExceptions (job deserialization etc).
/// Patches Result<'T>.Exception value in ResultCell.TryGetResult
/// Set by ResultCell.SetResultUnsafe
type ExceptionResultUnsafe = 
    | ExceptionResultUnsafe of exn

    static member inline CastToResult(result : ExceptionResultUnsafe) = 
        let (ExceptionResultUnsafe e) = result
        Exception(ExceptionDispatchInfo.Capture(e))

/// Azure store computation result container
[<DataContract; Sealed>]
type ResultCell<'T> internal (config : ConfigurationId, partitionKey : string, rowKey : string) = 
    
    [<DataMember(Name = "config")>]
    let config = config
    [<DataMember(Name = "partitionKey")>]
    let partitionKey = partitionKey
    [<DataMember(Name = "rowKey")>]
    let rowKey = rowKey

    [<IgnoreDataMember>]
    let mutable localCell : CacheAtom<Result<'T> option> option = None
    [<IgnoreDataMember>]
    let mutable localResult : Result<'T> option = None

    member self.GetCell() =
        match localCell with
        | Some lc -> lc
        | None ->
            let lc = CacheAtom.Create((fun () -> self.TryGetResult() |> Async.RunSync), intervalMilliseconds = 200)
            localCell <- Some lc
            lc

    member this.Path = sprintf "%s/%s" partitionKey rowKey

    member this.PartitionKey = partitionKey
    member this.RowKey = rowKey

    interface ICloudTask<'T> with
        member c.Id = partitionKey

        member c.AwaitResult(?timeout:int) = local {
            let! r = Cloud.OfAsync <| Async.WithTimeout(c.AwaitResult(), defaultArg timeout Timeout.Infinite)
            return r.Value
        }

        member c.TryGetResult() = local {
            let! r = Cloud.OfAsync <| c.TryGetResult()
            return r |> Option.map (fun r -> r.Value)
        }

        member c.IsCompleted = 
            match c.GetCell().Value with
            | Some(Completed _) -> true
            | _ -> false

        member c.IsFaulted =
            match c.GetCell().Value with
            | Some(Exception _) -> true
            | _ -> false

        member c.IsCanceled =
            match c.GetCell().Value with
            | Some(Cancelled _) -> true
            | _ -> false

        member c.Status =
            match c.GetCell().Value with
            | Some (Completed _) -> Tasks.TaskStatus.RanToCompletion
            | Some (Exception _) -> Tasks.TaskStatus.Faulted
            | Some (Cancelled _) -> Tasks.TaskStatus.Canceled
            | None -> Tasks.TaskStatus.Running

        member c.Result = 
            async {
                let! r = c.AwaitResult()
                return r.Value
            } |> Async.RunSync
        

    member __.SetResult(result : Result<'T>) : Async<unit> =
        async {
            let! bc = Blob.Create(config, partitionKey, guid(), fun () -> result)
            let uri = bc.Path
            let e = new BlobReferenceEntity(partitionKey, rowKey, uri.ToString(), ETag = "*")
            let! _ = Table.merge config config.RuntimeTable e
            return ()
        }

    // See static SetResultUnsafe, and ExceptionResultUnsafe
    member __.SetResultUnsafe(ex : Exception) : Async<unit> = async {
        return! ResultCell<'T>.SetResultUnsafe(config, partitionKey, rowKey, ex)
    }

    member __.TryGetResult() : Async<Result<'T> option> = 
        async {
            match localResult with
            | None ->
                let! e = Table.read<BlobReferenceEntity> config config.RuntimeTable partitionKey rowKey
                if String.IsNullOrEmpty e.Uri then return None
                else
                    let bc = Blob<Result<'T>>.FromPath(config, e.Uri)
                    let! result = Async.Catch <| bc.GetValue()
                    match result with
                    | Choice1Of2 v ->
                        localResult <- Some v
                        return localResult
                    | Choice2Of2 ex ->
                        let cellUnsafe = Blob<ExceptionResultUnsafe>.FromPath(config, e.Uri)
                        let! result = Async.Catch <| cellUnsafe.GetValue()
                        match result with
                        | Choice1Of2 result ->
                            let r = ExceptionResultUnsafe.CastToResult(result)
                            localResult <- Some r
                            return localResult
                        | Choice2Of2 _ ->
                            return! Async.Raise ex
            | Some _ -> return localResult
        }
    
    member __.AwaitResult() : Async<Result<'T>> = 
        async { 
            let! r = __.TryGetResult()
            match r with
            | None -> return! __.AwaitResult()
            | Some r -> return r
        }

    static member SetResultUnsafe(config : ConfigurationId, partitionKey, rowKey, e : exn) : Async<unit> =
        async {
            let! bc = Blob.Create(config, partitionKey, guid(), fun () -> ExceptionResultUnsafe(e))
            let uri = bc.Path
            let e = new BlobReferenceEntity(partitionKey, rowKey, uri.ToString(), ETag = "*")
            let! _ = Table.merge config config.RuntimeTable e
            return ()
        }

    static member FromPath(config : ConfigurationId, partitionKey : string, rowKey : string) = 
        new ResultCell<'T>(config, partitionKey, rowKey)

    static member Create(config, id, pid) = 
        let e = new BlobReferenceEntity(pid, id, null, EntityType = "RESULT")
        let op = TableOperation.Insert(e)
        { new TableResourceOperation<ResultCell<'T>> with
              member x.Operations = Seq.singleton op
              member x.Resource = new ResultCell<'T>(config, pid, id)
        }


[<DataContract; Sealed>]
type ResultAggregator<'T> internal (config : ConfigurationId, partitionKey : string, rowKey : string, size : int) = 
    static let mkRowKey name i = sprintf "%s:%010d" name i

    [<DataMember(Name = "config")>]
    let config = config
    [<DataMember(Name = "partitionKey")>]
    let partitionKey = partitionKey
    [<DataMember(Name = "rowKey")>]
    let rowKey = rowKey
    [<DataMember(Name = "size")>]
    let size = size

    let getEntities () = async {
        let pkFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey)
        let lower = mkRowKey rowKey 0
        let upper = mkRowKey rowKey size
        let rkFilter = 
            TableQuery.CombineFilters(
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual, upper),
                TableOperators.And,
                TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThanOrEqual, lower))

        let query = TableQuery<BlobReferenceEntity>().Where(TableQuery.CombineFilters(pkFilter, TableOperators.And, rkFilter))

        let! xs = Table.query<BlobReferenceEntity> config config.RuntimeTable query
        return xs
    }

    let completed () =
        async {
            let! xs = getEntities()
            return xs |> Seq.forall (fun e -> not <| String.IsNullOrEmpty(e.Uri))
        }

    member __.PartitionKey = partitionKey
    member __.RowKey = rowKey

    member __.TryGetResult(index : int) : Async<'T option> =
        async {
            if index >= size then return raise <| ArgumentOutOfRangeException("index", sprintf "Requested index %d, but size is %d" index size)

            let! entity = Table.read<BlobReferenceEntity> config config.RuntimeTable partitionKey (mkRowKey rowKey index)
            if String.IsNullOrEmpty entity.Uri then
                return None
            else
                let blob = Blob<'T>.FromPath(config, entity.Uri)
                let! value = blob.GetValue()
                return Some value
        }

    member __.SetResult(index : int, value : 'T) : Async<bool> = 
        async { 
            let e = new BlobReferenceEntity(partitionKey, mkRowKey rowKey index, null, ETag = "*")
            let! bc = Blob.Create(config, partitionKey, guid(), fun () -> value)
            e.Uri <- bc.Path
            let! _ = Table.merge config config.RuntimeTable e
            return! completed()
        }
    
    member __.Complete = Async.RunSync(completed())
    
    member __.ToArray() : Async<'T []> = 
        async { 
            if not __.Complete then 
                return! Async.Raise <| new InvalidOperationException("Result aggregator incomplete.")
            else
                let! xs = getEntities()
                let bs = 
                    xs
                    |> Seq.sortBy (fun x -> x.RowKey)
                    |> Seq.map (fun x -> x.Uri)
                    |> Seq.map (fun x -> Blob<_>.FromPath(config, x))
                    |> Seq.toArray

                let re = Array.zeroCreate<'T> bs.Length
                let i = ref 0
                for b in bs do
                    let! v = b.GetValue()
                    re.[!i] <- v
                    incr i
                return re
        }

    static member Get<'T>(config, pid, row, size) = new ResultAggregator<'T>(config, pid, row, size)

    static member Create<'T>(config, size, pid) = 
        let name = guid()
        let entities = seq {
            for i = 0 to size - 1 do
                let name = mkRowKey name i
                yield new BlobReferenceEntity(pid, name, String.Empty, EntityType = "AGGR")
        }
        let ops = entities |> Seq.map TableOperation.Insert
        { new TableResourceOperation<ResultAggregator<'T>> with
              member x.Operations = ops
              member x.Resource = new ResultAggregator<'T>(config, pid, name, size)
              
        }