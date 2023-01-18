module InvoiceNumberingReactor

type Service(numberService: InvoiceNumbering.Service, invoiceService: Invoice.Service) =
  member _.Handle(streamName, event) =
    async {
      match streamName with
      | FsCodec.StreamName.CategoryAndId(Invoice.Category, Invoice.InvoiceId.Parse invoiceId) ->
        match Invoice.Events.codec.TryDecode event with
        | ValueSome(Invoice.Events.InvoiceRaised _) ->
          let reserve () = numberService.ReserveNext invoiceId
          do! invoiceService.Number(invoiceId, reserve)
        | _ -> ()
      | _ -> ()
    }
