module Invoice

module Events =
  type InvoiceRaised = { InvoiceNumber: int; Payer: string; Amount: decimal}
  type Payment = { Amount: decimal }
  type EmailReceipt =  
    { IdempotencyKey: string; 
      Recipient: string; 
      SentAt: System.DateTimeOffset }

  type Event =
    | InvoiceRaised of InvoiceRaised
    | InvoiceEmailed of EmailReceipt
    | PaymentReceived of Payment
    | InvoiceFinalized
    // It is necessary to mark the Event DU with this interface for FsCodec
    interface TypeShape.UnionContract.IUnionContract

  let codec = FsCodec.SystemTextJson.Codec.Create<Event>()

module Fold =
  open Events
  
  type InvoiceState = 
    { Amount: decimal
      InvoiceNumber: int 
      EmailedTo: Set<string>
      AmountPaid: decimal
      Finalized: bool }

  type State = 
    | Initial 
    | Raised of InvoiceState 
  
  let initial = Initial

  let evolve state event =
    match state, event with
    | Initial, InvoiceRaised data -> 
      Raised { Amount = data.Amount; InvoiceNumber = data.InvoiceNumber; EmailedTo = Set.empty; AmountPaid = 0m; Finalized = false}
    | Raised s, InvoiceEmailed r -> Raised { s with EmailedTo = s.EmailedTo |> Set.add r.Recipient }
    | Raised s, PaymentReceived p -> Raised { s with AmountPaid = s.AmountPaid + p.Amount }
    | Raised s, InvoiceFinalized -> Raised { s with Finalized = true }
    | _ -> state
    
  let fold : State -> Event seq -> State = Seq.fold evolve

type Command =
  | RaiseInvoice of Events.InvoiceRaised
  | RecordEmailReceipt of Events.EmailReceipt
  | RecordPayment of Events.Payment
  | Finalize

let decide command state =
  match state, command with
  | Fold.Initial, RaiseInvoice data -> [ Events.InvoiceRaised data ]
  | _, RaiseInvoice _ -> []

  | Fold.Raised state, RecordEmailReceipt data 
      when not (state.EmailedTo |> Set.contains data.Recipient) -> 
        [Events.InvoiceEmailed data]
  | _, RecordEmailReceipt _ -> []

  | Fold.Raised state, RecordPayment data -> [ Events.PaymentReceived data ]
  | _, RecordPayment _ -> []

  | Fold.Raised state, Finalize -> [ Events.InvoiceFinalized ]
  | _, Finalize -> []

open FSharp.UMX
open System

type InvoiceId = Guid<invoiceId>
and [<Measure>] invoiceId

module InvoiceId =
  let inline parseGuid (g: Guid) : InvoiceId = %g
  let inline parse (s: string) = Guid.Parse s |> parseGuid
  let inline toGuid (id: InvoiceId) : Guid = %id
  let inline toString (id: InvoiceId) = (toGuid id).ToString("N")

let Category = "Invoice"
let streamId = Equinox.StreamId.gen InvoiceId.toString

type Service internal (resolve: InvoiceId -> Equinox.Decider<Events.Event, Fold.State>) =
  member _.Raise(id, data) =
    let decider = resolve id
    decider.Transact(decide (RaiseInvoice data))
    
  member _.RecordEmailReceipt(id, data) =
    let decider = resolve id
    decider.Transact(decide (RecordEmailReceipt data))
    
  member _.RecordPayment(id, data) =
    let decider = resolve id
    decider.Transact(decide (RecordPayment data))
    
  member _.Finalize(id) =
    let decider = resolve id
    decider.Transact(decide Finalize)

  member _.GetInvoice(id) =
    let decider = resolve id
    decider.Query(function Fold.Raised x -> Some x | _ -> None)


let create resolve = Service(streamId >> resolve Category)

