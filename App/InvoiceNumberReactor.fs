module InvoiceNumberReactor

open System
open Microsoft.Extensions.Hosting
open Propulsion.Internal
open Propulsion.MessageDb

type HostedService internal (connectionString, log, service: InvoiceNumberingReactor.Service, checkpointService: CheckpointStore.CheckpointService) =

  inherit BackgroundService()

  [<Literal>]
  let GroupName = "InvoiceNumberingReactor"

  let categories = [| Invoice.Category |]

  let checkpoints = CheckpointStore.CheckpointStore(checkpointService, GroupName, TimeSpan.FromSeconds 10)

  override _.ExecuteAsync(ct) =
    let computation =
      async {
        use sink = service.Sink(log)

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
