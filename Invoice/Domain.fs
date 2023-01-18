module Invoice

module Events =
  type InvoiceRaised =
    { Payer: string
      Amount: decimal }

  type Payment = { PaymentId: string; Amount: decimal }

  type EmailReceipt =
    { IdempotencyKey: string
      Recipient: string
      SentAt: System.DateTimeOffset }

  type Numbered = { InvoiceNumber: int }

  type Event =
    | InvoiceRaised of InvoiceRaised
    | InvoiceNumbered of Numbered
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
      InvoiceNumber: int option
      Payer: string
      EmailedTo: Set<string>
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
            InvoiceNumber = None
            Payer = data.Payer
            EmailedTo = Set.empty
            Payments = Set.empty
            AmountPaid = 0m }
      // We're guaranteed to not have two InvoiceRaised events and that it is the first event in the stream
      | e -> failwithf "Unexpected %A" e
    | Raised state ->
      match event with
      | InvoiceRaised _ as e -> failwithf "Unexpected %A" e
      | InvoiceNumbered data -> Raised { state with InvoiceNumber = Some data.InvoiceNumber }
      | InvoiceEmailed r -> Raised { state with EmailedTo = state.EmailedTo |> Set.add r.Recipient }
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

  let number invoiceNumber state =
    match state with
    | Fold.Raised { InvoiceNumber = None } -> [Events.InvoiceNumbered {InvoiceNumber = invoiceNumber}]
    | Fold.Raised { InvoiceNumber = Some n } when n = invoiceNumber -> []
    | Fold.Raised { InvoiceNumber = Some n } -> failwithf "Invoice is already numbered with a different number %d" n
    | Fold.Initial -> failwith "Invoice not found"
    | Fold.Finalized _ -> failwith "Invoice is finalized"

  let private hasSentEmailToRecipient recipient (state: Fold.InvoiceState) =
    state.EmailedTo |> Set.contains recipient

  let recordEmailReceipt (data: Events.EmailReceipt) state =
    match state with
    | Fold.Raised state when not (hasSentEmailToRecipient data.Recipient state) ->
      [ Events.InvoiceEmailed data ]
    | Fold.Raised _ -> []
    | Fold.Initial -> failwith "Invoice not found"
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

type InvoiceId = Guid<invoiceId>
and [<Measure>] invoiceId

module InvoiceId =
  let inline ofGuid (g: Guid) : InvoiceId = %g
  let inline parse (s: string) = Guid.Parse s |> ofGuid
  let (|Parse|) = parse
  let inline toGuid (id: InvoiceId) : Guid = %id
  // We choose the dashless N format to make the distinct parts of the stream's ID
  // easier for humans to read
  let inline toString (id: InvoiceId) = (toGuid id).ToString("N")

[<Literal>]
let Category = "Invoice"

let streamId = Equinox.StreamId.gen InvoiceId.toString

type InvoiceModel =
  { InvoiceNumber: int option
    Amount: decimal
    Payer: string
    EmailedTo: string array
    Finalized: bool }

module InvoiceModel =
  let fromState finalized (state: Fold.InvoiceState) =
    { InvoiceNumber = state.InvoiceNumber
      Amount = state.Amount
      Payer = state.Payer
      EmailedTo = state.EmailedTo |> Set.toArray
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

  member _.Number(id, data) =
    let decider = resolve id
    decider.Transact(Decisions.number data)

  member _.RecordEmailReceipt(id, data) =
    let decider = resolve id
    decider.Transact(Decisions.recordEmailReceipt data)

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
