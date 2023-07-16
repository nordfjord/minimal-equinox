module InvoiceNotificationEmail

open Equinox
open FsCodec.SystemTextJson
open Invoice
open Types

[<Literal>]
let Category = "InvoiceNotificationEmail"
let streamId = StreamId.gen2 InvoiceId.toString Email.toStreamId // assume the email will be hashed

module Events =
  type Event =
    | EmailSent of {| IdempotencyKey: string |}
    interface TypeShape.UnionContract.IUnionContract
  let codec = Codec.Create<Event>()

module Fold =
  type State = string option
  let initial = None
  let evolve _state event =
    match event with
    | Events.EmailSent x -> Some x.IdempotencyKey
  let fold: State -> Events.Event seq -> State = Seq.fold evolve


let renderEmail email amount =
  $"<h1>Invoice from Acme for ${amount}</h1>"

type Service(resolve: (InvoiceId * string) -> Decider<Events.Event, Fold.State>, mailer: IMailService) =
  member _.SendNotification(invoiceId: InvoiceId, email: string, amount: decimal) =
    let decider = resolve (invoiceId, email)
    let html = renderEmail email amount
    let subject = $"Invoice from Acme for ${amount}"
    // Assumption: this is an automated process, a different component
    // should handle the case when a user wants to resend etc.
    let idempotencyKey = sha1 (InvoiceId.toString invoiceId + email)
    decider.TransactAsync(fun state -> async {
      match state with
      | Some _ -> return (), [] // We've already sent the mail
      | None ->
        do! mailer.SendEmail({ To = email; Subject = subject; Body = html; IdempotencyKey = idempotencyKey })
        return (), [ Events.EmailSent {| IdempotencyKey = idempotencyKey |} ] })

let create mailer resolve = Service(streamId >> resolve Category, mailer)

