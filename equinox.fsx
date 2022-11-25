#r "nuget: FsCodec, *-*"
#r "nuget: FsCodec.SystemTextJson, *-*"
#r "nuget: TypeShape"

open System


module Events =
    type TimestampEvent = { Timestamp: DateTimeOffset }

    type Event =
        | CheckedIn of TimestampEvent
        | CheckedOut of TimestampEvent

        interface TypeShape.UnionContract.IUnionContract

    // In the previous example we created our own encode and decode functions
    // FsCodec can do this in an automated fashion
    let codec = FsCodec.SystemTextJson.Codec.Create<Event>()

module Fold =
    type State =
        { CheckedIn: DateTimeOffset option
          CheckedOut: DateTimeOffset option
          Completed: bool }

    let initial =
        { CheckedIn = None
          CheckedOut = None
          Completed = false }

    let evolve state event =
        match event with
        | Events.CheckedIn data ->
            { state with
                CheckedIn = Some data.Timestamp
                Completed = Option.isSome state.CheckedOut }
        | Events.CheckedOut data ->
            { state with
                CheckedOut = Some data.Timestamp
                Completed = Option.isSome state.CheckedIn }

    let fold: State -> Events.Event seq -> State = Seq.fold evolve


// While possible to create a DU for commands, it's unnecessary
module Commands =
    open Fold
    open Events

    let checkIn timestamp (state: State) =
        match state.CheckedIn with
        | None -> [ CheckedIn { Timestamp = timestamp } ]
        | Some ts when ts <> timestamp -> [ CheckedIn { Timestamp = timestamp } ]
        | Some _ -> []

    let checkOut timestamp (state: State) =
        match state.CheckedOut with
        | None -> [ CheckedOut { Timestamp = timestamp } ]
        | Some ts when ts <> timestamp -> [ CheckedOut { Timestamp = timestamp } ]
        | Some _ -> []


#r "nuget: Unquote"

open Swensen.Unquote

let (=>) events decide =
    events |> Fold.fold Fold.initial |> decide

let timestamp = DateTimeOffset.Parse("2022-10-11T13:00:00.000Z")
test <@ [] => Commands.checkIn timestamp = [ Events.CheckedIn { Timestamp = timestamp } ] @>
test <@ [ Events.CheckedIn { Timestamp = timestamp } ] => Commands.checkIn timestamp = [] @>


test <@ [] => Commands.checkOut timestamp = [ Events.CheckedOut { Timestamp = timestamp } ] @>
test <@ [ Events.CheckedOut { Timestamp = timestamp } ] => Commands.checkOut timestamp = [] @>


// This section contains zero-runtime cost branded identifiers using
// FSharp.UMX units of measure
#r "nuget: FSharp.UMX"
open FSharp.UMX

type TimingId = Guid<timingId>
and [<Measure>] timingId

module TimingId =
  let inline parseGuid (s: Guid) : TimingId = %(s)
  let inline parse (s: string) = s |> Guid.Parse |> parseGuid
  let inline toGuid (id: TimingId) : Guid = %id
  let inline toString (id: TimingId) = (toGuid id).ToString("N")

#r "nuget: Equinox, *-*"
open Equinox

let Category = "Timings"
let streamId = StreamId.gen TimingId.toString

// We make the constructor internal because this type should only ever be instantiated using the create method below
type Service internal (resolve: TimingId -> Decider<Events.Event, Fold.State>) =
    member _.CheckIn(id, timestamp) =
        let decider = resolve id
        decider.Transact(Commands.checkIn timestamp)

    member _.CheckOut(id, timestamp) =
        let decider = resolve id
        decider.Transact(Commands.checkOut timestamp)

    member _.GetTimings(id) =
        let decider = resolve id
        decider.Query(fun state -> state.CheckedIn, state.CheckedOut)

// This takes a while to grok
// We accept a resolve function whose type is: Category -> StreamId -> Decider<_,_>
// But that's a shitty API to use inside our Service. So we create a new `resolve`
// function which accepts a Guid and gives back a Guid
let create resolve = Service(streamId >> resolve Category)


#r "nuget: Equinox.MemoryStore, *-*"
// For better or worse Equinox has a serilog dependency
#r "nuget: Serilog"

Async.RunSynchronously(async {
    let store = Equinox.MemoryStore.VolatileStore<_>()
    let category =
        Equinox.MemoryStore.MemoryStoreCategory(store, Events.codec, Fold.fold, Fold.initial)
    let service = category |> Equinox.Decider.resolve Serilog.Log.Logger |> create

    let id = Guid.NewGuid() |> TimingId.parseGuid
    do! service.CheckIn(id, timestamp)
    let! state = service.GetTimings(id)
    test <@ state = (Some timestamp, None) @>
})


// Equinox has support for many stores, the above memory store example is only really
// applicable in testing scenarios.else

// The following example requires a running postgres with message-db
// run `docker run -d -p 5432:5432 ethangarofolo/message-db` to set up one up quickly

#r "nuget: Equinox.MessageDb, *-*"

Async.RunSynchronously(async {
    let connectionString = "Host=localhost; Database=message_store; Username=message_store"
    let connector = Equinox.MessageDb.MessageDbConnector(connectionString)
    let ctx = Equinox.MessageDb.MessageDbContext(connector.Establish(), batchSize = 500)


    let cache = Equinox.Cache("test", sizeMb = 10)
    // Each store in equinox has different Caching strategies and different access
    // strategies available. A sliding cache window represents a sensible default 
    let caching = Equinox.MessageDb.CachingStrategy.SlidingWindow(cache, TimeSpan.FromMinutes 20)
    let category = Equinox.MessageDb.MessageDbCategory(ctx, Events.codec, Fold.fold, Fold.initial, caching)


    let service = category |> Equinox.Decider.resolve Serilog.Log.Logger |> create
    let id = Guid.NewGuid() |> TimingId.parseGuid
    do! service.CheckIn(id, timestamp)
    let! state = service.GetTimings(id)
    test <@ state = (Some timestamp, None) @>
})

