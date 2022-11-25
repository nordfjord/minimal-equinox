open System

// The domain for this example is a system receiving
// check-in and check-out commands from a queue
// The problem is they can arrive in any order
// The system therefore emits a "completed" event once both have arrived


// 1. We keep our event DU in a module
module Events =
    type TimestampEvent = { Timestamp: DateTimeOffset }

    type Event =
        | CheckedIn of TimestampEvent
        | CheckedOut of TimestampEvent

// 2. The Fold module is responsible for transforming a sequence of events to a current state
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


// As you can see the domain is testable without thinking about storage
// By convention our domain is unaware of identity which makes for an easier time testing as well

// Once we have factored our codebase into this form then the next step is to wire it up to storage
// and expose it through http or some other mechanism

type IEventStore =
    abstract member ReadStream:
        streamName: string * fromVersion: int64 -> Async<int64 * (string * ReadOnlyMemory<byte>) array>

    abstract member WriteEvents:
        streamName: string * events: (string * ReadOnlyMemory<byte>) array * expectedVersion: int64 option ->
            Async<int64>

// A naive transact implementation
// Load the stream, fold the events into state, make decsision, append new events
let transact encode tryDecode (client: IEventStore) fold initial streamName decide =
    async {
        let! version, events = client.ReadStream(streamName, 0L)
        let decoded = events |> Array.choose tryDecode
        let state = fold initial decoded
        let newEvents = decide state |> List.map encode |> Array.ofList
        return! client.WriteEvents(streamName, newEvents, Some version)
    }

let memoryStore () =
    let events = ResizeArray()

    { new IEventStore with
        member _.ReadStream(streamName, version) =
            async {
                let streamEvents =
                    events
                    |> Seq.filter (fun (s, _, _) -> s = streamName)
                    |> Seq.map (fun (_, t, c) -> t, c)
                    |> Array.ofSeq

                return streamEvents.Length, streamEvents |> Array.skip (int version)
            }

        member _.WriteEvents(streamName, toAppend, expectedVersion) =
            async {
                let streamEvents =
                    events
                    |> Seq.filter (fun (s, _, _) -> s = streamName)
                    |> Seq.map (fun (_, t, c) -> t, c)
                    |> Array.ofSeq

                match expectedVersion with
                | Some e when e <> streamEvents.Length -> failwith "Wrong expected version"
                | _ -> ()

                events.AddRange(toAppend |> Array.map (fun (t, c) -> streamName, t, c))
                return int64 (streamEvents.Length + toAppend.Length)
            } }


let store = memoryStore ()

// In order to interact with storage we'll need to transform our F# domain events
// into whatever format our underlying storage supports
// Here we're hand rolling encode and decode functions that emit and accept
// a tuple of event type and data
let encode event =
    match event with
    | Events.CheckedIn data -> "CheckedIn", System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(data) |> ReadOnlyMemory
    | Events.CheckedOut data ->
        "CheckedOut", System.Text.Json.JsonSerializer.SerializeToUtf8Bytes(data) |> ReadOnlyMemory

let tryDecode (t, data: ReadOnlyMemory<byte>) =
    match t with
    | "CheckedIn" -> Events.CheckedIn(System.Text.Json.JsonSerializer.Deserialize data.Span) |> Some
    | "CheckedOut" -> Events.CheckedOut(System.Text.Json.JsonSerializer.Deserialize data.Span) |> Some
    | _ -> None

Async.RunSynchronously(async {
    do!
        transact encode tryDecode store Fold.fold Fold.initial "Test-123" (Commands.checkIn timestamp)
        |> Async.Ignore

    let! version, events = store.ReadStream("Test-123", 0L)
    printfn "%A" (version, events)
})

// We can add optimizations to our transact such as caching

open System.Collections.Generic

let transactWithCache
    encode
    tryDecode
    (client: IEventStore)
    (cache: Dictionary<_, _>)
    (fold: 'a -> 'b seq -> 'a)
    initial
    streamName
    decide
    = 
    async {
        let cachedVersion, cachedState =
            match cache.TryGetValue(streamName) with
            | true, v -> v
            | _ -> 0L, initial

        let! version, events = client.ReadStream(streamName, cachedVersion)
        let decoded = events |> Array.choose tryDecode
        let state = fold cachedState decoded
        let newEvents = decide state
        let! newVersion = client.WriteEvents(streamName, newEvents |> List.map encode |> Array.ofList, Some version)
        let newState = fold state newEvents
        cache[streamName] <- newVersion, newState
        return newVersion
    }

Async.RunSynchronously(async {
    let cache = Dictionary()
    let trx = transactWithCache encode tryDecode store cache Fold.fold Fold.initial

    do!
        trx "Test-123" (Commands.checkIn (timestamp + TimeSpan.FromHours 1))
        |> Async.Ignore

    test <@ cache.ContainsKey("Test-123") @>

    do!
        trx "Test-123" (Commands.checkOut (timestamp + TimeSpan.FromHours 2))
        |> Async.Ignore

    let version, events = cache["Test-123"]
    printfn "%A" (version, events)
})

// The good news is that you don't have to maintain your own transact functions
// The Equinox suite of tools has affordances for
// - Managing encoding and decoding (via FsCodec) 
// - Transacting against a store (via Equinox and its stores)
// - Reacting to events (via Propulsion and its sources and sinks)
//
// See 2 - equinox.fsx for how this same example would be achieved in equinox
