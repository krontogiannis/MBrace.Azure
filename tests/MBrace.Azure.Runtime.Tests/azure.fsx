﻿#I "../../bin/"
#r "FsPickler.dll"
#r "Vagabond.dll"
#r "Microsoft.ServiceBus.dll"
#r "Microsoft.WindowsAzure.Storage.dll"
#r "System.ServiceModel"
#r "System.Runtime.Serialization"
#time "on"


// Playground - using directly azure primitives
open System
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob
open Microsoft.ServiceBus
open Microsoft.ServiceBus.Messaging

let selectEnv name =
    (Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User),
        Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Machine))
    |> function | null, s | s, null | s, _ -> s


let store  = selectEnv "azurestorageconn"
let sbus = selectEnv "azureservicebusconn" 


let getRandomName () =
    // See http://blogs.msdn.com/b/jmstall/archive/2014/06/12/azure-storage-naming-rules.aspx
    let alpha = [|'a'..'z'|]
    let alphaNumeric = Array.append alpha [|'0'..'9'|]
    let maxLen = 63
    let randOf =
        let rand = new Random(int DateTime.Now.Ticks)
        fun (x : char []) -> x.[rand.Next(0, x.Length)]

    let name = 
        [| yield randOf alpha
           for _i = 1 to maxLen-1 do yield randOf alphaNumeric |]
    new String(name)

open Microsoft.WindowsAzure.Storage.Table

let acc = CloudStorageAccount.Parse(store)
let client = acc.CreateCloudTableClient()
let name = getRandomName()
let table = client.GetTableReference(name)
table.CreateIfNotExists()


(table.CreateIfNotExistsAsync()).ContinueWith(fun _ -> ())
|> Async.AwaitTask
|> Async.RunSynchronously




let p = new System.Net.NetworkInformation.Ping()
p.Send("google.com")



let ns = NamespaceManager.CreateFromConnectionString(sbus)
let topic = "topictest"
let td = new TopicDescription(topic)
ns.CreateTopic(td)

let client = TopicClient.CreateFromConnectionString(sbus, topic)

let sub s = 
    let sd = new SubscriptionDescription(topic, s)
    let expr = "foo = '" + s + "' OR foo = ''"
    let filter = new SqlFilter(expr)
    ns.CreateSubscription(sd, filter)
    SubscriptionClient.CreateFromConnectionString(sbus, topic, s)

let s1 = sub "sub1"
let s2 = sub "sub2"

let bm = new BrokeredMessage("1")
bm.Properties.Add("foo", "sub1")
client.Send(bm)

let bm = new BrokeredMessage("2")
bm.Properties.Add("foo", "")
client.Send(bm)


s1.Receive()
s2.Receive()


let ns = NamespaceManager.CreateFromConnectionString(sbus)
let qd = new QueueDescription("tmp")
qd.LockDuration <- TimeSpan.FromMinutes(2.)
ns.CreateQueue(qd) |> ignore
let queue = QueueClient.CreateFromConnectionString(sbus, "tmp")


let m = new BrokeredMessage("Hello")
m.Properties.Add("foo", "bar")
m.Properties.Add("goo", "bar")
m.Size
m.
queue.Send(m) //(200 - 68)

queue.SendBatch([m]) //(200 - 68)

let ms = Array.init 1500 (fun i -> new BrokeredMessage(sprintf "Hello %d" i))
ms |> Seq.sumBy (fun m -> m.Size)

queue.SendBatch(ms)
queue.PrefetchCount
qd.MessageCount

let ms' = queue.ReceiveBatch(1000)
ms' |> Seq.iter (fun msg -> printfn "%s" <| msg.GetBody<string>(); msg.Complete())


queue.Send(new BrokeredMessage("Hello world"))


let bm = queue.Receive()

bm.RenewLock()

queue.Complete(bm.LockToken)

//
//type AutoRenewLease (blob : ICloudBlob) =
//    // Acquire infinite lease
//    let proposed = System.Guid.NewGuid() |> string
//    let leaseId = blob.AcquireLease(Unchecked.defaultof<_> , proposed)
//    
//    interface IDisposable with
//        member this.Dispose(): unit = blob.ReleaseLease(AccessCondition.GenerateLeaseCondition(leaseId))
//            
//    member this.HasLock = true
//    member this.ProposedId = proposed
//    member this.LeaseId = leaseId

let acc = CloudStorageAccount.Parse("")
let client = acc.CreateCloudBlobClient()
let cont = client.GetContainerReference("foo")
cont.CreateIfNotExists()
cont.ListBlobs()


Async.Parallel [ acc.CreateCloudTableClient().GetTableReference("bar").CreateIfNotExistsAsync()
                 |> Async.AwaitTask ]
|> Async.RunSynchronously

client.ListContainers()
let container = client.GetRootContainerReference()

container.ListBlobs()

let br = container.GetBlockBlobReference("foo/bar.txt")
br.UploadText("Hello world")


container.Name
container.ListBlobs()

let container = client.GetContainerReference("temp")
container.CreateIfNotExists()

let container = client.GetContainerReference("temp")
let b = container.GetBlockBlobReference("foollll.txt")

b.Exists()
b.FetchAttributes()

open System.Diagnostics
let time f msg =
    let sw = Stopwatch.StartNew()
    f()
    sw.Stop()
    printfn "%s : %A" msg sw.Elapsed

let niter = 100
for i = 0 to niter do //25 sec
    let client = acc.CreateCloudBlobClient()
    let container = client.GetContainerReference("temp")
    let b = container.GetBlockBlobReference("foo.txt")
    b.UploadText("Hello world")

for i = 0 to niter do // 35
    let client = acc.CreateCloudBlobClient()
    let container = client.GetContainerReference("temp")
    let b = container.GetBlockBlobReference("foo.txt")
    b.UploadText("Hello world")
    
    let client = acc.CreateCloudBlobClient()
    let container = client.GetContainerReference("temp")
    let b = container.GetBlockBlobReference("foo.txt")
    b.FetchAttributes()


for i = 0 to niter do // 34
    let client = acc.CreateCloudBlobClient()
    let container = client.GetContainerReference("temp")
    let b = container.GetBlockBlobReference("foo.txt")
    b.UploadText("Hello world")
    b.FetchAttributes()

b.Properties
b.DownloadToStream()
let s = b.OpenWrite()
s.Write([|1uy..10uy|],0, 10)
s.Dispose()

b.UploadText("Hello world")



[ 1..10 ]
|> List.map (fun i -> container.GetBlockBlobReference("foo.txt"))
|> List.mapi (fun b i -> async { return i.UploadText(String.init (1024 * 10) (fun _ ->string b)) } )
|> Async.Parallel
|> Async.Ignore
|> Async.RunSynchronously



blob1.DownloadText()
blob2.DownloadText()
blob1.Delete()
let ac1 = AccessCondition.GenerateEmptyCondition()

blob1.UploadText()
let l1 = new AutoRenewLease(blob1)

