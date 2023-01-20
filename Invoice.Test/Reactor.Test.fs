module Reactor.Test

open System
open Xunit
open Serilog
open Swensen.Unquote

let resolve log (codec, fold, initial) store =
  Equinox.MemoryStore.MemoryStoreCategory(store, codec, fold, initial)
  |> Equinox.Decider.resolve log

let makeServices () =
  let store = Equinox.MemoryStore.VolatileStore()
  let log = LoggerConfiguration().WriteTo.Console().CreateLogger()

  let reactor = InvoiceNumberingReactor.createMem store log
  let invoiceService = Invoice.createMem store log

  let categoryF s = s = Invoice.Category

  let sink = reactor.Sink(log)

  let source =
    Propulsion.MemoryStore.MemoryStoreSource<_>(log, store, categoryF, id, sink)

  source, invoiceService

// This is a fairly costly test, running it 10 times should be sufficient
[<Fact>]
let ``An invoice number is reserved`` () =
  task {
    let id = Guid.NewGuid() |> Invoice.InvoiceId.ofGuid
    let source, invoiceService = makeServices ()
    let _ = source.Start()

    do! invoiceService.Raise(id, { Payer = "1"; Amount = 33m })
    do! source.Monitor.AwaitCompletion()

    let! invoice = invoiceService.ReadInvoice(id)

    test <@ invoice |> Option.bind (fun x -> x.InvoiceNumber) = Some 1 @>
  }
