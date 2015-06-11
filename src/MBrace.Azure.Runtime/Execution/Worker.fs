﻿namespace MBrace.Azure.Runtime

open System
open System.Diagnostics
open System.Threading

open Nessos.FsPickler
open Nessos.Vagabond
open Nessos.Vagabond.AppDomainPool

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Runtime.Vagabond
open MBrace.Azure
open MBrace.Azure.Runtime
open MBrace.Azure.Runtime.Utilities
open MBrace.Azure.Runtime.Primitives
open MBrace.Runtime.Utils

type internal WorkerConfig = 
    { State                     : RuntimeState
      MaxConcurrentJobs         : int
      Resources                 : ResourceRegistry
      JobEvaluatorConfiguration : JobEvaluatorConfiguration
      Logger                    : ICloudLogger
      JobEvaluator              : JobEvaluator }

type private WorkerMessage =
    | Start of WorkerConfig  * AsyncReplyChannel<unit>
    | Update of WorkerConfig * AsyncReplyChannel<unit>
    | Stop of AsyncReplyChannel<unit>
    | IsActive of AsyncReplyChannel<bool>

type private WorkerState =
    | Idle
    | Running of WorkerConfig * AsyncReplyChannel<unit>

type internal Worker () =

    let mutable currentJobCount = 0
    
    let workerLoopAgent =
        /// Timeout for the Mailbox loop.
        let receiveTimeout = 100
        /// TryDequeue errors sleep interval.
        let onErrorWaitTime = 1000

        let waitForPendingJobs (config : WorkerConfig) = async {
            config.Logger.Log "Stop requested. Waiting for pending jobs."
            let rec wait () = async {
                if currentJobCount > 0 then
                    do! Async.Sleep receiveTimeout
                    return! wait ()
            }
            do! wait ()
            config.Logger.Log "No active jobs."
            config.Logger.Log "Unregister current worker."
            do! config.State.WorkerManager.SetCurrentAsStopped()
            config.Logger.Log "Worker stopped."
        }

        new MailboxProcessor<WorkerMessage>(fun inbox ->
            // queueFault indicated if last dequeue action resulted in exception

            let rec workerLoop queueFault (state : WorkerState) = async {
                let! message = inbox.TryReceive(10)

                match message, state with
                | None, Running(config, _) ->
                    if currentJobCount >= config.MaxConcurrentJobs then
                        do! Async.Sleep receiveTimeout
                        return! workerLoop false state
                    else
                        let! job = Async.Catch <| config.State.TryDequeue()
                        match job with
                        | Choice1Of2 None -> 
                            if queueFault then 
                                config.Logger.Log "Reverting state to Running"
                                do! config.State.WorkerManager.SetCurrentAsRunning()
                                config.Logger.Log "Done"
                            return! workerLoop false state
                        | Choice1Of2(Some message) ->
                            // run isolated job
                            let jc = Interlocked.Increment &currentJobCount
                            if queueFault then 
                                config.Logger.Log "Reverting state to Running"
                                do! config.State.WorkerManager.SetCurrentAsRunning()
                                config.Logger.Logf "Done"
                            config.State.WorkerManager.SetJobCountLocal(jc)
                            config.Logger.Logf "Increase Dequeued Jobs %d/%d" jc config.MaxConcurrentJobs
                            let! _ = Async.StartChild <| async { 
                                try
                                    config.Logger.Logf "Dequeued message %A" message.JobId
                                    let! res = Async.Catch <| config.State.JobManager.Update(message.ProcessId, message.JobId, JobStatus.Dequeued, config.State.WorkerManager.Current.Id, message.DeliveryCount)
                                    match res with
                                    | Choice2Of2 ex ->
                                        config.Logger.Logf "Failed to update message %A state :\n%A\nCalling Abandon." message.JobId ex
                                        do! config.State.JobQueue.AbandonAsync(message)
                                    | Choice1Of2 _ -> 
                                        config.Logger.Log "Downloading PickledJob"
                                        let! pickledJob = Async.Catch <| message.GetPayloadAsync<PickledJob>()
                                        match pickledJob with
                                        | Choice2Of2 ex ->
                                            config.Logger.Logf "Failed to download PickledJob :\n%A" ex
                                            return! FaultHandler.FaultMessageAsync(message, config.State, ex)
                                        | Choice1Of2 pickledJob ->
                                            config.Logger.Log "Downloading assemblies locally"     
                                            let! localAssemblies = Async.Catch <| config.State.AssemblyManager.DownloadDependencies pickledJob.Dependencies

                                            match localAssemblies with
                                            | Choice1Of2 localAssemblies ->                                    
                                                let! ch = config.JobEvaluator.EvaluateAsync(config.JobEvaluatorConfiguration, localAssemblies, message, pickledJob)
                                                match ch with
                                                | Choice1Of2 () -> return ()
                                                | Choice2Of2 e  -> config.Logger.Logf "Unhandled exception : %A" e
                                            | Choice2Of2 ex ->
                                                config.Logger.Logf "Failed to download PickledJob or dependencies:\n%A" ex
                                                do! FaultHandler.FaultPickledJobAsync(pickledJob, message, config.State, ex)
                                finally
                                    let jc = Interlocked.Decrement &currentJobCount
                                    config.State.WorkerManager.SetJobCountLocal(jc)
                                    config.Logger.Logf "Decrease Dequeued Jobs %d/%d" jc config.MaxConcurrentJobs
                            }
                            return! workerLoop false state
                        | Choice2Of2 ex ->
                            config.Logger.Logf "Worker JobQueue fault\n%A" ex
                            config.State.WorkerManager.SetCurrentAsFaulted(ex)
                            do! Async.Sleep onErrorWaitTime
                            return! workerLoop true state
                | None, Idle -> 
                    do! Async.Sleep receiveTimeout
                    return! workerLoop false state
                | Some(Start(config, handle)), Idle ->
                    config.State.JobQueue.AcceptMessages()
                    return! workerLoop false (Running(config, handle))
                | Some(Stop ch), Running(config, handle) ->
                    do! waitForPendingJobs config
                    ch.Reply ()
                    handle.Reply()
                    return! workerLoop false Idle
                | Some(Update(config,ch)), Running _ ->
                    config.Logger.Log "Updating worker configuration."
                    return! workerLoop false (Running(config, ch))
                | Some(IsActive ch), Idle ->
                    ch.Reply(false)
                    return! workerLoop false state
                | Some(IsActive ch), Running _ ->
                    ch.Reply(true)
                    return! workerLoop false state
                | Some(Start _), _  ->
                    return invalidOp "Called Start, but worker is not Idle."
                | _, Idle ->
                    return invalidOp "Worker is Idle."
            }
            workerLoop false Idle
        )

    do workerLoopAgent.Start()

    member __.IsActive = workerLoopAgent.PostAndReply(IsActive)

    member __.Start(configuration : WorkerConfig) =
        workerLoopAgent.PostAndReply(fun ch -> Start(configuration, ch) )
        
    member __.Stop() =
        workerLoopAgent.PostAndReply(fun ch -> Stop(ch))

    member __.Restart(configuration) =
        __.Stop()
        __.Start(configuration)