// This is my least favourite part of propulsion
// Just give me System.Diagnostics.Metrics already!

open System
open System.Diagnostics
open Equinox
open Equinox.MessageDb
open Propulsion.MessageDb
open Propulsion.Sinks
open Serilog
open Types

let log = LoggerConfiguration().WriteTo.Console().CreateLogger()
let noopLogger = Serilog.Log.Logger

type Stats(log, statsInterval, stateInterval) =
   inherit Propulsion.Streams.Stats<unit>(log, statsInterval, stateInterval)
   override _.HandleOk(_) = ()
   override _.HandleExn(log, exn) = log.Information(exn, "Unhandled")

[<Literal>]
let GroupName = "InvoiceNotifier"

let cache = Equinox.Cache("test", sizeMb = 50)

let defaultConnString =
  "Host=localhost; Database=message_store; Username=message_store"

let writeUrl =
  Environment.tryGetEnv "MESSAGE_DB_URL" |> Option.defaultValue defaultConnString
let readUrl =
  Environment.tryGetEnv "MESSAGE_DB_REPLICA_URL" |> Option.defaultValue writeUrl
let checkpointsUrl =
  Environment.tryGetEnv "CHECKPOINTS_DB_URL" |> Option.defaultValue "Host=localhost; Database=message_store; Username=postgres"

let connection = MessageDbClient(writeUrl, readUrl)
let context = MessageDbContext(connection)
let caching = CachingStrategy.SlidingWindow(cache, TimeSpan.FromMinutes(20))

let mailer =
  { new IMailService with
      member _.SendEmail(x) = async { return () } }

let invoiceMailerService =
  MessageDbCategory(context, InvoiceNotificationEmail.Events.codec, InvoiceNotificationEmail.Fold.fold, InvoiceNotificationEmail.Fold.initial, caching)
  |> Decider.resolve noopLogger
  |> InvoiceNotificationEmail.create mailer

let invoiceService =
  MessageDbCategory(context, Invoice.Events.codec, Invoice.Fold.fold, Invoice.Fold.initial, caching)
  |> Decider.resolve noopLogger
  |> Invoice.create


module Handler =
  let (|InvoiceId|_|) = function
    | FsCodec.StreamName.CategoryAndId(Invoice.Category, id) -> Some(Types.InvoiceId.parse id)
    | _ -> None

  let (|Raised|_|) (events: _ array) =
    // We know that InvoiceRaised is always the first event in the stream so there's no reason
    // To check any other event
    match Invoice.Events.codec.TryDecode events[0] with
    | ValueSome(Invoice.Events.InvoiceRaised data) -> Some data
    | _ -> None

  let handle streamName (events: Propulsion.Sinks.Event[]) = async {
    match streamName, events with
    | InvoiceId(id), Raised(data) ->
        do! invoiceMailerService.SendNotification(id, data.Payer, data.Amount)
        return StreamResult.AllProcessed, ()
    | _ -> return StreamResult.AllProcessed, () }

let stats = Stats(noopLogger, TimeSpan.FromMinutes 1, TimeSpan.FromMinutes 1)

let checkpoints =
  ReaderCheckpoint.CheckpointStore(checkpointsUrl, "message_store", GroupName, TimeSpan.FromSeconds 10)

checkpoints.CreateSchemaIfNotExists().Wait()

[<EntryPoint>]
let main argv = Async.RunSynchronously(async {
  use listener = new ActivityListener()
  listener.Sample <- fun _ -> ActivitySamplingResult.AllDataAndRecorded
  listener.ShouldListenTo <- fun source -> source.Name = "Equinox"
  listener.ActivityStopped <- fun span ->
    match span.DisplayName with
    | "TrySync" ->
      log.Information("{name} ({ms}ms) count={count}; stream={stream}; version={version}",
                      span.DisplayName, span.Duration.TotalMilliseconds,
                      span.GetTagItem("eqx.count"), span.GetTagItem("eqx.stream_name"), span.GetTagItem("eqx.new_version"))
    | "Load" ->
      log.Information("{name} {load_method} ({ms}ms) cached={cache_hit}; count={count}; batches={batches} stream={stream}",
                      span.DisplayName, span.GetTagItem("eqx.load_method"), span.Duration.TotalMilliseconds,
                      span.GetTagItem("eqx.cache_hit"), span.GetTagItem("eqx.count"), span.GetTagItem("eqx.batches"), span.GetTagItem("eqx.stream_name"))
    | _ ->
      log.Information("{name} ({ms}ms)", span.DisplayName, span.Duration.TotalMilliseconds)
  ActivitySource.AddActivityListener(listener)

  use sink = Propulsion.Sinks.Factory.StartConcurrent(
     noopLogger,
     maxReadAhead = 100,
     maxConcurrentStreams = 10,
     handle = Handler.handle,
     stats = stats
   )

  use source =
    MessageDbSource(
      noopLogger,
      statsInterval = TimeSpan.FromMinutes 1,
      connectionString = readUrl,
      batchSize = 500,
      tailSleepInterval = TimeSpan.FromSeconds 1,
      checkpoints = checkpoints,
      sink = sink,
      categories = [| Invoice.Category |]
    )
      .Start()
  do! source.AwaitWithStopOnCancellation()
  return 0 })

