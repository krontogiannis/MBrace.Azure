﻿namespace MBrace.Azure.Client

#nowarn "52"

open MBrace.Azure.Runtime.Info
open MBrace.Azure.Runtime.Primitives
open MBrace.Runtime.Utils.PrettyPrinters
open System
open Microsoft.FSharp.Linq.NullableOperators
open MBrace.Azure
open System.Text
open MBrace.Azure.Runtime.Utilities

type internal JobReporter() = 
    static let onNull (value : Nullable<'T>) = 
        if value.HasValue then value.Value.ToString() else "N/A" 

    static let template : Field<JobInfo> list = 
        [ Field.create "Id" Left (fun p -> p.Id)
          Field.create "Type" Right (fun p -> p.JobType)
          Field.create "Status" Right (fun p -> sprintf "%A" p.Status)
          Field.create "Return Type" Left (fun p -> p.ReturnType) 
          Field.create "Timestamp" Left (fun p -> p.Timestamp)
          Field.create "Size" Left (fun p -> getHumanReadableByteSize p.JobSize)
          Field.create "Delivery Count" Left (fun p -> p.DeliveryCount)
          Field.create "Initialization" Left (fun p -> onNull p.InitializationTime) 
          Field.create "CompletionTime" Left (fun p -> onNull p.CompletionTime) 
        ]
    
    static member Report(jobs : JobInfo seq, title, borders) = 
        let ps = jobs 
                 |> Seq.sortBy (fun p -> p.Timestamp, p.Status, p.JobType)
                 |> Seq.toList
        Record.PrettyPrint(template, ps, title, borders)

    static member ReportTreeView(jobs : JobInfo seq, title) =
        let sb = new StringBuilder()
        let _ = sb.AppendLine(title)

        let shorten (id : string) = sprintf "%s...%s" <| id.Substring(0, 7) <| id.Substring(id.Length - 3)

        let root = jobs |> Seq.find (fun j -> j.JobType = Root)

        let child (current : JobInfo) =
            jobs |> Seq.filter (fun j -> j.ParentId = current.Id)

        let rec treeview (current : JobInfo) depth : unit =
            if depth > 0 then
                for i = 0 to 4 * (depth-1) - 1 do 
                    ignore <| sb.Append(if i % 4 = 0 then '|' else ' ')
                let _ = sb.Append("├───") // fancy
                ()
            let _ = sb.AppendLine(sprintf "%s %O %+A" (shorten current.Id) current.JobType current.Status)
            child current |> Seq.iter (fun j -> treeview j (depth + 1))
        treeview root 0
        sb.ToString()


type internal ProcessReporter() = 
    static let template : Field<ProcessRecord> list = 
        [ Field.create "Name" Left (fun p -> p.Name)
          Field.create "Process Id" Right (fun p -> p.Id)
          Field.create "Status" Right (fun p -> p.State)
          Field.create "Completed" Left (fun p -> p.Completed)
          Field.create "Execution Time" Left (fun p -> if p.Completed.GetValueOrDefault() then p.CompletionTime ?-? p.InitializationTime else DateTimeOffset.UtcNow -? p.InitializationTime)
          Field.create "Jobs" Center (fun p -> sprintf "%3d / %3d / %3d / %3d"  p.ActiveJobs.Value p.FaultedJobs.Value p.CompletedJobs.Value p.TotalJobs.Value)
          Field.create "Result Type" Left (fun p -> p.TypeName) 
          Field.create "Start Time" Left (fun p -> p.InitializationTime)
          Field.create "Completion Time" Left (fun p -> p.CompletionTime )
        ]
    
    static member Report(processes : ProcessRecord seq, title, borders) = 
        let ps = processes 
                 |> Seq.sortBy (fun p -> p.InitializationTime.Value)
                 |> Seq.toList
        sprintf "%s\nJobs : Active / Faulted / Completed / Total\n" <| Record.PrettyPrint(template, ps, title, borders)

type internal WorkerReporter() = 
    static let template : Field<WorkerRef> list = 
        let double_printer (value : double) = 
            if value < 0. then "N/A" else sprintf "%.1f" value
        [ Field.create "Id" Left (fun p -> p.Id)
          Field.create "Status" Left (fun p -> string p.Status)
          Field.create "% CPU / Cores" Center (fun p -> sprintf "%s / %d" (double_printer p.CPU) p.ProcessorCount)
          Field.create "% Memory / Total(MB)" Center (fun p -> 
            let memPerc = 100. * p.Memory / p.TotalMemory |> double_printer
            sprintf "%s / %s" memPerc <| double_printer p.TotalMemory)
          Field.create "Network(ul/dl : KB/s)" Center (fun n -> sprintf "%s / %s" <| double_printer n.NetworkUp <| double_printer n.NetworkDown)
          Field.create "Jobs" Center (fun p -> sprintf "%d / %d" p.ActiveJobs p.MaxJobCount)
          Field.create "Hostname" Left (fun p -> p.Hostname)
          Field.create "Process Id" Right (fun p -> p.ProcessId)
          Field.create "Heartbeat" Left (fun p -> p.HeartbeatTime)
          Field.create "Initialization Time" Left (fun p -> p.InitializationTime) 
        ]
    
    static member Report(workers : WorkerRef seq, title, borders) = 
        let ws = workers
                 |> Seq.sortBy (fun w -> w.InitializationTime)
                 |> Seq.toList
        Record.PrettyPrint(template, ws, title, borders)

type internal LogReporter() = 
    static let template : Field<LogRecord> list = 
        [ Field.create "Source" Left (fun p -> p.PartitionKey)
          Field.create "Timestamp" Right (fun p -> let pt = p.Time in pt.ToString("ddMMyyyy HH:mm:ss.fff zzz"))
          Field.create "Message" Left (fun p -> p.Message) ]
    
    static member Report(logs : LogRecord seq, title, borders) = 
        let ls = logs 
                 |> Seq.sortBy (fun l -> l.Time, l.PartitionKey)
                 |> Seq.toList
        Record.PrettyPrint(template, ls, title, borders)
