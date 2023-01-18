module Program

open System
open Microsoft.AspNetCore.Builder
open Microsoft.Extensions.Hosting
open Microsoft.Extensions.DependencyInjection
open Equinox.MessageDb
open Serilog

module Environment =
  let tryGetEnv = Environment.GetEnvironmentVariable >> Option.ofObj

let log = LoggerConfiguration().WriteTo.Console().CreateLogger()

let cache = Equinox.Cache("test", sizeMb = 50)

let defaultConnString =
  "Host=localhost; Database=message_store; Username=message_store"

let writeUrl =
  Environment.tryGetEnv "MESSAGE_DB_URL" |> Option.defaultValue defaultConnString

let readUrl =
  Environment.tryGetEnv "MESSAGE_DB_REPLICA_URL" |> Option.defaultValue writeUrl

let connection = MessageDbClient(writeUrl, readUrl)
let context = MessageDbContext(connection)
let caching = CachingStrategy.SlidingWindow(cache, TimeSpan.FromMinutes(20))

type Services() =
  static member Create(context, codec, fold, initial, ?access) =
    MessageDbCategory(context, codec, fold, initial, caching, ?access = access)

  static member InvoiceService(context) =
    let codec = Invoice.Events.codec
    let fold, initial = Invoice.Fold.fold, Invoice.Fold.initial
    Services.Create(context, codec, fold, initial)
    |> Equinox.Decider.resolve log
    |> Invoice.create

  static member InvoiceNumberService(context) =
    let codec = InvoiceNumbering.Events.codec
    let fold, initial = InvoiceNumbering.Fold.fold, InvoiceNumbering.Fold.initial
    let snapshot = InvoiceNumbering.Fold.snapshotEventType, InvoiceNumbering.Fold.toSnapshot
    let access = AccessStrategy.AdjacentSnapshots snapshot
    Services.Create(context, codec, fold, initial, access)
    |> Equinox.Decider.resolve log
    |> InvoiceNumbering.create

  static member InvoiceNumberReactorService(context) =
    let invoiceService = Services.InvoiceService(context)
    let numberService = Services.InvoiceNumberService(context)
    InvoiceNumberingReactor.Service(numberService, invoiceService)

  static member CheckpointService(context) =
    let codec = CheckpointStore.Events.codec
    let fold, initial = CheckpointStore.Fold.fold, CheckpointStore.Fold.initial
    let access = AccessStrategy.LatestKnownEvent
    Services.Create(context, codec, fold, initial, access)
    |> Equinox.Decider.resolve log
    |> CheckpointStore.create

  static member HostedReactor(context) =
    let reactorService = Services.InvoiceNumberReactorService(context)
    let checkpointService = Services.CheckpointService(context)
    new HostedReactor.HostedReactor(readUrl, log, reactorService, checkpointService)

let builder = WebApplication.CreateBuilder()
builder.Services.AddHostedService(fun _ -> Services.HostedReactor(context)) |> ignore
let app = builder.Build()

let service = Services.InvoiceService(context)

app.MapPost("/", Func<_, _>(fun body -> task {
  let id = Guid.NewGuid() |> Invoice.InvoiceId.ofGuid
  do! service.Raise(id, body)
  return id
})) |> ignore

app.MapPost("/{id}/finalize", Func<_, _>(fun id -> task {
  do! service.Finalize(id)
  return "OK"
})) |> ignore

app.MapPost("/{id}/record-payment", Func<_, _, _>(fun id payment -> task {
  do! service.RecordPayment(id, payment)
  return "OK"
})) |> ignore

app.MapGet("/{id}", Func<_, _>(fun id -> task {
  return! service.ReadInvoice(id)
})) |> ignore

app.Run()
