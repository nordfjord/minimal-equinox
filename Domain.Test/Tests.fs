module Tests

open Invoice
open Swensen.Unquote
open FsCheck.Xunit
open FsCodec.Core

[<Property>]
let ``The event codec round-trips cleanly`` event =
  let encoded = Events.codec.Encode((), event)
  let saved = TimelineEvent.Create(0L, encoded.EventType, encoded.Data)
  let decoded = Events.codec.TryDecode(saved)
  test <@ ValueSome event = decoded @>

let (=>) events interpret =
  Fold.fold Fold.initial events |> interpret

open Events

[<Property>]
let ``Raising an invoice`` data =
  test <@ [] => Decisions.raiseInvoice data = [ InvoiceRaised data ] @>
  // test the idempotency
  test <@ [ InvoiceRaised data ] => Decisions.raiseInvoice data = [] @>
  // A finalized invoice will throw
  raises <@ [ InvoiceRaised data; InvoiceFinalized ] => Decisions.raiseInvoice data @>

[<Property>]
let ``Recording payments`` raised data =
  raises <@ [] => Decisions.recordPayment data @>
  test <@ [ InvoiceRaised raised ] => Decisions.recordPayment data = [ PaymentReceived data ] @>
  test <@ [ InvoiceRaised raised; PaymentReceived data ] => Decisions.recordPayment data = [] @>
  raises <@ [ InvoiceRaised raised; InvoiceFinalized ] => Decisions.recordPayment data @>

[<Property>]
let ``Finalizing`` raised =
  raises <@ [] => Decisions.finalize @>
  test <@ [ InvoiceRaised raised ] => Decisions.finalize = [ InvoiceFinalized ] @>
  test <@ [ InvoiceRaised raised; InvoiceFinalized ] => Decisions.finalize = [] @>
