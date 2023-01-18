module InvoiceNumbering

module Events =
  type Event =
    | InvoiceNumberReserved of
      {| invoiceNumber: int
         reservedFor: Invoice.InvoiceId |}
    | Snapshotted of
      {| next: int
         allocated: Map<Invoice.InvoiceId, int> |}

    interface TypeShape.UnionContract.IUnionContract

  let codec = FsCodec.SystemTextJson.Codec.Create<Event>()

module Fold =
  open Events

  type State =
    { next: int
      allocated: Map<Invoice.InvoiceId, int> }

  let initial: State = { next = 1; allocated = Map.empty }

  let evolve state event =
    match event with
    | Snapshotted data ->
      { next = data.next
        allocated = data.allocated }
    | InvoiceNumberReserved data ->
      { next = data.invoiceNumber + 1
        allocated = Map.add data.reservedFor data.invoiceNumber state.allocated }

  let fold: State -> Event seq -> State = Seq.fold evolve
  let toSnapshot s = Snapshotted {| next = s.next; allocated = s.allocated |}
  let snapshotEventType = nameof Snapshotted

module Decide =
  open Fold

  let reserve reservedFor state =
    match Map.tryFind reservedFor state.allocated with
    | None ->
      state.next,
      [ Events.InvoiceNumberReserved
          {| invoiceNumber = state.next
             reservedFor = reservedFor |} ]
    | Some n -> n, []

[<Literal>]
let Category = "InvoiceNumber"
// This is a global stream for the entire system
// In the future there might be one stream per tenant
// In which case we'd gen a streamId from the tenant id
let streamId = Equinox.StreamId.gen (fun () -> "0")

type Service(resolve: unit -> Equinox.Decider<Events.Event, Fold.State>) =
  member _.ReserveNext(reserveFor) =
    let decider = resolve ()
    decider.Transact(Decide.reserve reserveFor)

let create resolve = Service(streamId >> resolve Category)
