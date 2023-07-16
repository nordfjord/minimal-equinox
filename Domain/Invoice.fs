module Invoice

open Types

module Events =
  type InvoiceRaised =
    { InvoiceNumber: int
      Payer: string
      Amount: decimal }

  type Payment = { PaymentId: string; Amount: decimal }

  type Event =
    | InvoiceRaised of InvoiceRaised
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
      Payer: string
      Payments: Set<string>
      AmountPaid: decimal }

  type State =
    | Initial
    | Raised of InvoiceState
    | Finalized of InvoiceState

  let initial = Initial

  let evolve state event =
    match state with
    | Initial ->
      match event with
      | InvoiceRaised data ->
        Raised
          { Amount = data.Amount
            InvoiceNumber = data.InvoiceNumber
            Payer = data.Payer
            Payments = Set.empty
            AmountPaid = 0m }
      // We're guaranteed to not have two InvoiceRaised events and that it is the first event in the stream
      | e -> failwithf "Unexpected %A" e
    | Raised state ->
      match event with
      | InvoiceRaised _ as e -> failwithf "Unexpected %A" e
      | PaymentReceived p ->
        Raised
          { state with
              AmountPaid = state.AmountPaid + p.Amount
              Payments = state.Payments |> Set.add p.PaymentId }
      | InvoiceFinalized -> Finalized state
    // A Finalized invoice is terminal. No further events should be appended
    | Finalized _ -> failwithf "Unexpected %A" event

  let fold: State -> Event seq -> State = Seq.fold evolve

module Decisions =
  let raiseInvoice data state =
    match state with
    | Fold.Initial -> [ Events.InvoiceRaised data ]
    // This is known as an idempotency check. We could be receiving the same
    // command due to a retry, in which case it is not considered a failure
    // since the Fold will already be in the state that this command should put it in
    | Fold.Raised state when state.Amount = data.Amount && state.Payer = data.Payer -> []
    | Fold.Raised _ -> failwith "Invoice is already raised"
    | Fold.Finalized _ -> failwith "Invoice is finalized"

  let recordPayment (data: Events.Payment) state =
    match state with
    | Fold.Raised state when state.Payments |> Set.contains data.PaymentId -> []
    | Fold.Raised _ -> [ Events.PaymentReceived data ]
    | Fold.Finalized _ -> failwith "Invoice is finalized"
    | Fold.Initial -> failwith "Invoice not found"

  let finalize state =
    match state with
    | Fold.Finalized _ -> []
    | Fold.Raised _ -> [ Events.InvoiceFinalized ]
    | Fold.Initial -> failwith "Invoice not found"

open FSharp.UMX
open System

[<Literal>]
let Category = "Invoice"

let streamId = Equinox.StreamId.gen InvoiceId.toString

type InvoiceModel =
  { InvoiceNumber: int
    Amount: decimal
    Payer: string
    Finalized: bool }

module InvoiceModel =
  let fromState finalized (state: Fold.InvoiceState) =
    { InvoiceNumber = state.InvoiceNumber
      Amount = state.Amount
      Payer = state.Payer
      Finalized = finalized }

module Queries =
  let summary =
    function
    | Fold.Initial -> None
    | Fold.Raised invoice -> Some(InvoiceModel.fromState false invoice)
    | Fold.Finalized invoice -> Some(InvoiceModel.fromState true invoice)

type Service internal (resolve: InvoiceId -> Equinox.Decider<Events.Event, Fold.State>) =
  member _.Raise(id, data) =
    let decider = resolve id
    decider.Transact(Decisions.raiseInvoice data)

  member _.RecordPayment(id, data) =
    let decider = resolve id
    decider.Transact(Decisions.recordPayment data)

  member _.Finalize(id) =
    let decider = resolve id
    decider.Transact(Decisions.finalize)

  member _.ReadInvoice(id) =
    let decider = resolve id
    decider.Query(Queries.summary)

let create resolve = Service(streamId >> resolve Category)
