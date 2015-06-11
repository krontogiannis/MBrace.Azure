namespace MBrace.Azure.Client

#nowarn "52"

open MBrace.Azure.Runtime.Info
open MBrace.Azure.Runtime.Primitives
open MBrace.Runtime.Utils.PrettyPrinters
open System
open Microsoft.FSharp.Linq.NullableOperators
open MBrace.Azure
open System.Text
open MBrace.Azure.Runtime.Utilities

[<AutoOpen>]
module private Helpers =
    let protect<'R> (proj : 'R -> string) = fun u -> try proj u with _ -> "Evaluation failed"

type internal JobReporter() = 
    static let optionToString (value : Option<'T>) = 
        match value with 
        | Some value -> value.ToString() 
        | None -> "N/A" 

    static let template : Field<Job> list = 
        [ Field.create "Id" Left (protect (fun p -> p.Id))
          Field.create "Type" Right (protect(fun p -> sprintf "%A" p.JobType))
          Field.create "Status" Right (protect(fun p -> sprintf "%A" p.Status))
          Field.create "Returns" Left (protect(fun p -> p.ReturnType))
          Field.create "Size" Left (protect(fun p -> getHumanReadableByteSize p.JobSize))
          Field.create "Deliveries" Left (protect(fun p -> string p.DeliveryCount))
          Field.create "Execution Time" Left (protect(fun p ->
            match p.CompletionTime, p.DequeueTime with
            | Some t, Some t' -> string(t - t')
            | None, Some t -> string(DateTimeOffset.UtcNow - t)
            | _ -> "N/A"))
          Field.create "Posted" Left (protect(fun p -> string p.CreationTime))
          Field.create "Dequeued" Left (protect(fun p -> optionToString p.DequeueTime))
          Field.create "Started" Left (protect(fun p -> optionToString p.StartTime))
          Field.create "Completed" Left (protect(fun p -> optionToString p.CompletionTime))
          Field.create "Timestamp" Left (protect(fun p -> string p.Timestamp))
        ]
    
    static member Report(jobs : Job seq, title) = 
        let ps = jobs 
                 |> Seq.sortBy (fun p -> p.CreationTime, p.Timestamp)
                 |> Seq.toList
        let sb = 
            jobs 
            |> Seq.countBy (fun j -> j.Status)
            |> Seq.sortBy fst
            |> Seq.fold (fun (sb : StringBuilder) (s,i) -> sb.AppendFormat("{0} : {1}\n", s , i)) (StringBuilder().AppendLine(title))

        sb.AppendLine()
          .AppendLine(Record.PrettyPrint(template, ps)).ToString()

type internal WorkerReporter() = 
    static let template : Field<WorkerRef> list = 
        let double_printer (value : double) = 
            if value < 0. then "N/A" else sprintf "%.1f" value
        [ Field.create "Id" Left (protect(fun p -> p.Id))
          Field.create "Status" Left (protect(fun p -> string p.Status))
          Field.create "% CPU / Cores" Center (protect(fun p -> sprintf "%s / %d" (double_printer p.CPU) p.ProcessorCount))
          Field.create "% Memory / Total(MB)" Center (protect(fun p -> 
            let memPerc = 100. * p.Memory / p.TotalMemory |> double_printer
            sprintf "%s / %s" memPerc <| double_printer p.TotalMemory))
          Field.create "Network(ul/dl : KB/s)" Center (protect(fun n -> sprintf "%s / %s" <| double_printer n.NetworkUp <| double_printer n.NetworkDown))
          Field.create "Jobs" Center (protect(fun p -> sprintf "%d / %d" p.ActiveJobs p.MaxJobCount))
          Field.create "Hostname" Left (protect(fun p -> p.Hostname))
          Field.create "Process Id" Right (protect(fun p -> string p.ProcessId))
          Field.create "Heartbeat" Left (protect(fun p -> string p.HeartbeatTime))
          Field.create "Initialization Time" Left (protect(fun p -> string p.InitializationTime))
        ]
    
    static member Report(workers : WorkerRef seq, title, borders) = 
        let ws = workers
                 |> Seq.sortBy (fun w -> w.InitializationTime)
                 |> Seq.toList
        Record.PrettyPrint(template, ws, title, borders)

type internal LogReporter() = 
    static let template : Field<LogRecord> list = 
        [ Field.create "Source" Left (protect(fun p -> p.PartitionKey))
          Field.create "Timestamp" Right (protect(fun p -> let pt = p.Time in pt.ToString("ddMMyyyy HH:mm:ss.fff zzz")))
          Field.create "Message" Left (protect(fun p -> p.Message)) ]
    
    static member Report(logs : LogRecord seq, title, borders) = 
        let ls = logs 
                 |> Seq.sortBy (fun l -> l.Time, l.PartitionKey)
                 |> Seq.toList
        Record.PrettyPrint(template, ls, title, borders)
