module Reactor.Test

open System
open Xunit
open Serilog
open Swensen.Unquote

let resolve (codec, fold, initial) store =
  Equinox.MemoryStore.MemoryStoreCategory(store, codec, fold, initial)
  |> Equinox.Decider.resolve Serilog.Log.Logger

let makeServices () =
  let store = Equinox.MemoryStore.VolatileStore()
  let log = LoggerConfiguration().WriteTo.Console().CreateLogger()

  let numberService =
    Equinox.MemoryStore.MemoryStoreCategory(
      store,
      InvoiceNumbering.Events.codec,
      InvoiceNumbering.Fold.fold,
      InvoiceNumbering.Fold.initial
    )
    |> Equinox.Decider.resolve log
    |> InvoiceNumbering.create

  let invoiceService =
    Equinox.MemoryStore.MemoryStoreCategory(
      store,
      Invoice.Events.codec,
      Invoice.Fold.fold,
      Invoice.Fold.initial
    )
    |> Equinox.Decider.resolve log
    |> Invoice.create

  let reactor = InvoiceNumberingReactor.Service(numberService, invoiceService)

  let categoryF s = s = Invoice.Category

  let sink = reactor.Sink(log)

  let source =
    Propulsion.MemoryStore.MemoryStoreSource<_>(log, store, categoryF, id, sink)

  source, invoiceService

// This is a fairly costly test, running it 10 times should be sufficient
[<Fact>]
let ``An invoice number is reserved`` () =
  async {
    let id = Guid.NewGuid() |> Invoice.InvoiceId.ofGuid
    let source, invoiceService = makeServices ()
    let _ = source.Start()

    do! invoiceService.Raise(id, { Payer = "1"; Amount = 33m })
    do! source.Monitor.AwaitCompletion()

    let! invoice = invoiceService.ReadInvoice(id)

    test <@ invoice |> Option.bind (fun x -> x.InvoiceNumber) = Some 1 @>
  }
