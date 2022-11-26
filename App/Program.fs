module Program

open System
open Microsoft.AspNetCore.Builder
open Microsoft.Extensions.Hosting
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

let connection = MessageDbConnector(writeUrl, readUrl).Establish()
let context = MessageDbContext(connection)
let caching = CachingStrategy.SlidingWindow(cache, TimeSpan.FromMinutes(20))

let service =
  MessageDbCategory(context, Invoice.Events.codec, Invoice.Fold.fold, Invoice.Fold.initial, caching)
  |> Equinox.Decider.resolve log
  |> Invoice.create

let builder = WebApplication.CreateBuilder()
let app = builder.Build()

let raiseInvoice body =
  task {
    let id = Guid.NewGuid() |> Invoice.InvoiceId.ofGuid
    do! service.Raise(id, body)
    return id
  }

app.MapPost("/", Func<_, _>(raiseInvoice)) |> ignore

let finalizeInvoice id =
  task {
    do! service.Finalize(id)
    return "OK"
  }

app.MapPost("/{id}/finalize", Func<_, _>(finalizeInvoice)) |> ignore

let recordPayment id payment =
  task {
    do! service.RecordPayment(id, payment)
    return "OK"
  }

app.MapPost("/{id}/record-payment", Func<_, _, _>(recordPayment)) |> ignore

let readInvoice id =
  task { return! service.ReadInvoice(id) }

app.MapGet("/{id}", Func<_, _>(readInvoice)) |> ignore

app.Run()
