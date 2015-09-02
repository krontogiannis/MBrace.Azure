﻿namespace MBrace.Azure.Tests

open System.Threading

open NUnit.Framework
open FsUnit

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Azure.Runtime
open MBrace.Azure

#nowarn "445"

open MBrace.Core.Tests

[<AutoOpenAttribute>]
module Utils =
    open System

    let private selectEnv name =
        (Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User),
          Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Machine),
            Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Process))
        |> function 
           | s, _, _ when not <| String.IsNullOrEmpty(s) -> s
           | _, s, _ when not <| String.IsNullOrEmpty(s) -> s
           | _, _, s when not <| String.IsNullOrEmpty(s) -> s
           | _ -> failwithf "Variable %A not found" name

    let remoteConfig = new Configuration(selectEnv "azurestorageconn", selectEnv "azureservicebusconn")
    let emulatorConfig = new Configuration("UseDevelopmentStorage=true", selectEnv "azureservicebusconn")


module Choice =

    let shouldEqual (value : 'T) (input : Choice<'T, exn>) = 
        match input with
        | Choice1Of2 v' -> should equal value v'
        | Choice2Of2 e -> should equal value e

    let shouldMatch (pred : 'T -> bool) (input : Choice<'T, exn>) =
        match input with
        | Choice1Of2 t when pred t -> ()
        | Choice1Of2 t -> raise <| new AssertionException(sprintf "value '%A' does not match predicate." t)
        | Choice2Of2 e -> should be instanceOfType<'T> e

    let shouldFailwith<'T, 'Exn when 'Exn :> exn> (input : Choice<'T, exn>) = 
        match input with
        | Choice1Of2 t -> should be instanceOfType<'Exn> t
        | Choice2Of2 e -> should be instanceOfType<'Exn> e


type RuntimeSession(config : MBrace.Azure.Configuration, localWorkers : int) =

    static do MBraceCluster.LocalWorkerExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.azureworker.exe"
    
    let mutable state = None

    member __.Start () =
        match state with
        | Some _ -> invalidOp "MBrace runtime already initialized."
        | None -> 
            let runtime = 
                if localWorkers < 1 then
                    MBraceCluster.GetHandle(config, logger = ConsoleLogger(), logLevel = LogLevel.Debug)
                else
                    MBraceCluster.InitOnCurrentMachine(config, localWorkers, maxJobs = 32, logger = ConsoleLogger(), logLevel = LogLevel.Debug)
            state <- Some runtime

    member __.Stop () =
        state |> Option.iter (fun r -> (r.KillAllLocalWorkers() ; r.Reset(true, true, true, true, true, true, false)))
        state <- None

    member __.Runtime =
        match state with
        | None -> invalidOp "MBrace runtime not initialized."
        | Some r -> r