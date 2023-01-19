module InvoiceNumberingReactor

open System

type Service(numberService: InvoiceNumbering.Service, invoiceService: Invoice.Service) =
  member _.Handle(struct (streamName, events)) =
    async {
      match streamName with
      | FsCodec.StreamName.CategoryAndId(Invoice.Category, Invoice.InvoiceId.Parse invoiceId) ->
        for event in events do
          match Invoice.Events.codec.TryDecode event with
          | ValueSome(Invoice.Events.InvoiceRaised _) ->
            let reserve () = numberService.ReserveNext invoiceId
            do! invoiceService.Number(invoiceId, reserve)
          | _ -> ()
      | _ -> ()

      return struct (Propulsion.Streams.SpanResult.AllProcessed, ())
    }

  member this.Sink(log) =
    let stats =
      { new Propulsion.Streams.Stats<_>(log, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1) with
          member _.HandleOk x = ()
          member _.HandleExn(log, x) = () }

    Propulsion.Streams.Default.Config.Start(
      log,
      maxReadAhead = 100,
      maxConcurrentStreams = 12,
      handle = this.Handle,
      stats = stats,
      statsInterval = TimeSpan.FromMinutes 1
    )
