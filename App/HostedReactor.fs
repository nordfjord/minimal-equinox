module HostedReactor

open System
open Microsoft.Extensions.Hosting
open Propulsion.Internal
open Propulsion.MessageDb

type HostedReactor internal (connectionString, log, service: InvoiceNumberingReactor.Service, checkpointService: CheckpointStore.CheckpointService) =
  inherit BackgroundService()

  let stats =
    { new Propulsion.Streams.Stats<_>(log, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1) with
        member _.HandleOk x = ()
        member _.HandleExn(log, x) = () }

  [<Literal>]
  let GroupName = "InvoiceNumberingReactor"

  let categories = [| Invoice.Category |]

  let checkpoints = CheckpointStore.CheckpointStore(checkpointService, GroupName, TimeSpan.FromSeconds 10)
  let maxReadAhead = 100
  let maxConcurrentStreams = 2

  let handle struct(stream, events) =
    async {
      for event in events do
        do! service.Handle(stream, event)

      return struct (Propulsion.Streams.SpanResult.AllProcessed, ())
    }

  override _.ExecuteAsync(ct) =
    let computation =
      async {
        use sink =
          Propulsion.Streams.Default.Config.Start(
            log,
            maxReadAhead,
            maxConcurrentStreams,
            handle,
            stats,
            TimeSpan.FromMinutes 1
          )

        use src =
          MessageDbSource(
            log,
            statsInterval = TimeSpan.FromMinutes 1,
            connectionString = connectionString,
            batchSize = 1000,
            // Controls the time to wait once fully caught up
            // before requesting a new batch of events
            tailSleepInterval = TimeSpan.FromMilliseconds 100,
            checkpoints = checkpoints,
            sink = sink,
            // An array of message-db categories to subscribe to
            // Propulsion guarantees that events within streams are
            // handled in order, it makes no guarantees across streams (Even within categories)
            categories = categories
          )
            .Start()

        do! src.AwaitWithStopOnCancellation()
      }

    Async.StartAsTask(computation, cancellationToken = ct)
