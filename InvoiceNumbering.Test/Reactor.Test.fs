module Reactor.Test

open System
open FsCheck
open FsCheck.Xunit
open Swensen.Unquote

let resolve (codec, fold, initial) store =
  Equinox.MemoryStore.MemoryStoreCategory(store, codec, fold, initial)
  |> Equinox.Decider.resolve Serilog.Log.Logger

let makeServices () =
  let store = Equinox.MemoryStore.VolatileStore()
  let log = Serilog.Log.Logger

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
[<Property(MaxTest = 10)>]
let ``An invoice number is reserved`` (NonEmptySet ids) =
  async {
    let source, invoiceService = makeServices ()
    use _ = source.Start()

    do!
      ids
      |> Seq.map (fun id -> invoiceService.Raise(id, { Payer = "1"; Amount = 33m }))
      |> Async.Sequential
      |> Async.Ignore<unit array>

    do! source.Monitor.AwaitCompletion()

    let! invoices = ids |> Seq.map invoiceService.ReadInvoice |> Async.Sequential

    let numbers =
      invoices
      |> Array.choose (function
        | Some x -> x.InvoiceNumber
        | None -> None)
      |> Array.sort

    test <@ numbers = Array.ofSeq (seq { 1 .. Array.length invoices }) @>
  }
