module InvoiceNumberingReactor

open System

type Stats(log, statsInterval, stateInterval) =
  inherit Propulsion.Streams.Stats<unit>(log, statsInterval, stateInterval)
  override _.HandleOk _ = printfn "Processed event"
  override _.HandleExn(log, exn) = log.Information(exn, "Unhandled")

type Service(numberService: InvoiceNumbering.Service, invoiceService: Invoice.Service) =

  member private _.Reserve(struct (invoiceId, event)) =
    match Invoice.Events.codec.TryDecode event with
    | ValueSome(Invoice.Events.InvoiceRaised _) ->
      let reserve () = numberService.ReserveNext invoiceId
      invoiceService.Number(invoiceId, reserve)
    | _ -> async { () }

  member this.Handle(struct (streamName, events)) =
    async {
      match streamName with
      | FsCodec.StreamName.CategoryAndId(Invoice.Category, Invoice.InvoiceId.Parse invoiceId) ->
        for event in events do
          do! this.Reserve(invoiceId, event)
      | _ -> ()

      return struct (Propulsion.Streams.SpanResult.AllProcessed, ())
    }

  member this.Sink(log) =
    let stats = Stats(log, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1)

    Propulsion.Streams.Default.Config.Start(
      log,
      maxReadAhead = 100,
      maxConcurrentStreams = 1,
      handle = this.Handle,
      stats = stats,
      statsInterval = TimeSpan.FromMinutes 1
    )
