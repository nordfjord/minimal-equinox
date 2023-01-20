module CheckpointStore

open System.Threading.Tasks
open Equinox
open Propulsion.Feed

module Events =
  type Event =
    | Checkpoint of {| pos: int64 |}
    interface TypeShape.UnionContract.IUnionContract

  let codec = FsCodec.SystemTextJson.Codec.Create<Event>()

module Fold =
  type State = int64 option
  let initial = None

  let evolve _ =
    function
    | Events.Checkpoint x -> Some x.pos

  let fold: State -> Events.Event seq -> State = Seq.fold evolve

let streamId = StreamId.gen2 SourceId.toString id
type CheckpointService internal (resolve: string -> StreamId -> Equinox.Decider<Events.Event, Fold.State>) =
  member _.SetCheckpoint(source, tranche, group: string, pos) =
    let category = TrancheId.toString tranche + ":position"
    let streamId = streamId (source, group)
    let decider = resolve category streamId
    decider.Transact (function
          | None -> [ Events.Checkpoint {| pos = pos |} ]
          | Some curr when curr < pos -> [ Events.Checkpoint {| pos = pos |} ]
          | Some _ -> [])

  member _.ReadCheckpoint(source, tranche, group) =
    let category = TrancheId.toString tranche + ":position"
    let streamId = StreamId.ofRaw $"{SourceId.toString source}_{group}"
    let decider = resolve category streamId
    decider.Query(id)

let create resolve = CheckpointService(resolve)

type CheckpointStore(service: CheckpointService, consumerGroup, defaultCheckpointFrequency) =
  interface IFeedCheckpointStore with
    member this.Commit(source, tranche, pos, ct) =
        Async.StartImmediateAsTask(service.SetCheckpoint(source, tranche, consumerGroup, Position.toInt64 pos), cancellationToken = ct)

    member this.Start(source, tranche, establishOrigin, ct) =
      task {
        let! maybePos =
          Async.StartImmediateAsTask(service.ReadCheckpoint(source, tranche, consumerGroup), cancellationToken = ct)

        let! pos =
          match maybePos, establishOrigin with
          | Some pos, _ -> Task.FromResult (Position.parse pos)
          | None, Some f -> f.Invoke ct
          | None, None -> Task.FromResult Position.initial

        return struct (defaultCheckpointFrequency, pos)
      }
