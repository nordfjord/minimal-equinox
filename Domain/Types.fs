module Types

open System
open System.Text
open FSharp.UMX

type InvoiceId = Guid<invoiceId>
and [<Measure>] invoiceId

module InvoiceId =
  let inline ofGuid (g: Guid) : InvoiceId = %g
  let inline parse (s: string) = Guid.Parse s |> ofGuid
  let inline toGuid (id: InvoiceId) : Guid = %id
  // We choose the dashless N format to make the distinct parts of the stream's ID
  // easier for humans to read
  let inline toString (id: InvoiceId) = (toGuid id).ToString("N")



let sha1 (str: string) =
    (str
    |> Encoding.UTF8.GetBytes
    |> System.Security.Cryptography.SHA1.HashData
    |> Array.take 16
    |> Guid)
      .ToString("N")

module Email =
    let inline toStreamId (email: string) = sha1 email

type SendEmailDto =
   { To: string
     Subject: string
     Body: string
     IdempotencyKey: string }

type IMailService =
  abstract member SendEmail: SendEmailDto -> Async<unit>

module Environment =
  let tryGetEnv = Environment.GetEnvironmentVariable >> Option.ofObj
