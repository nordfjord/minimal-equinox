module Tests

open FsCheck.Xunit
open Swensen.Unquote

let makeService () =
  let store = Equinox.MemoryStore.VolatileStore()

  Equinox.MemoryStore.MemoryStoreCategory(
    store,
    InvoiceNumbering.Events.codec,
    InvoiceNumbering.Fold.fold,
    InvoiceNumbering.Fold.initial
  )
  |> Equinox.Decider.resolve Serilog.Log.Logger
  |> InvoiceNumbering.create

[<Property>]
let ``A reserved number is not doled out to another`` (invoiceId) (ids) =
  async {
    let service = makeService ()
    let! myNum = service.ReserveNext(invoiceId)

    let! others =
      ids
      |> List.filter ((<>) invoiceId)
      |> List.map service.ReserveNext
      |> Async.Sequential

    test <@ others |> Array.contains myNum = false @>
  }
