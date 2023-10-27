open StdLabels
open MoreLabels
module IntSet = Set.Make(Int)

type event_param =
  | Start_workout
  | Complete_workout
  | Phase_progression of int
  | PlayVideo
  | ReadTip
  | Other of string
[@@deriving show]

let event_param_of string =
  match string with
  | "start_workout" -> Start_workout
  | "complete_workout" -> Complete_workout
  (* Quick and easy as we we just have 3 phases *)
  | "phase_progression_1" -> Phase_progression 1
  | "phase_progression_2" -> Phase_progression 2
  | "phase_progression_3" -> Phase_progression 3
  | "read_tip" -> ReadTip
  | "playVideo" -> PlayVideo
  | other -> Other other

let event_param_to_string event_param =
  match event_param with
  | Start_workout -> "start_workout"
  | Complete_workout -> "complete_workout"
  | Phase_progression d -> Format.sprintf "phase_progression_%d" d
  | PlayVideo -> "playVideo"
  | ReadTip -> "read_tip"
  | Other str -> str

type event_log = {
  id : int;
  user_id : int;
  timestamp : Ptime.t;
  param : event_param;
  value : string option;
  session_id : string option;
  new_event : bool;
}
[@@deriving show]

(* FIXME: Could rely on postgresql timer

*)
let counter = ref 2_000_000_000

let next_id () =
  let return = !counter in
  counter := return + 1;
  return

let make_new_complete last_event =
  let delta = Ptime.Span.of_d_ps (0, Int64.of_int 1_000_000) |> Option.get in
  let timestamp = Ptime.add_span last_event.timestamp delta |> Option.get in
  let user_id = last_event.user_id in
  let new_event =
    {
      id = next_id ();
      param = Complete_workout;
      value = None;
      session_id = None;
      user_id;
      timestamp;
      new_event = true;
    }
  in
  (* Format.eprintf "Creating new event:\n%a\n" pp_event_log new_event; *)
  new_event

(*

  Small state machine with each event_log being transitions between states

  *)
module EventValidate = struct
  let rec state_init state events =
    match events with
    | [] -> state
    | event :: remaining -> (
        match event.param with
        | Start_workout -> state_start_workout (event :: state) remaining
        | Complete_workout | Phase_progression _ | PlayVideo | ReadTip ->
            (* events that are out of order, all worksouts must begin with start
          *)
            state_init (event :: state) remaining
        | Other s -> failwith ("Unknown event: " ^ s))

  and state_start_workout state events =
    match events with
    | [] -> state
    | event :: remaining -> (
        match event.param with
        | Start_workout ->
            (* Previous start event has has no phase_progress event *)
            state_start_workout (event :: state) remaining
        | Phase_progression _ ->
            state_phrase_progression (event :: state) remaining
        | Complete_workout ->
            (* Complete without phases cannot have a close event *)
            state_init (event :: state) remaining
        | PlayVideo | ReadTip ->
            (* We are still reading the start of a workout *)
            state_start_workout (event :: state) remaining
        | Other s -> failwith ("Unknown event: " ^ s))

  and state_phrase_progression state events =
    match events with
    | [] ->
        let previous_event = List.hd state in
        let new_complete = make_new_complete previous_event in
        new_complete :: state
    | event :: remaining -> (
        match event.param with
        | Start_workout ->
            let previous_event = List.hd state in
            let new_complete = make_new_complete previous_event in
            let events = event :: new_complete :: state in
            state_start_workout events remaining
        | Complete_workout -> state_init (event :: state) remaining
        | Phase_progression _ | PlayVideo | ReadTip ->
            state_phrase_progression (event :: state) remaining
        | Other s -> failwith ("Unknown event: " ^ s))

  let start events = state_init [] events
end

let clean s = String.(sub s ~pos:1 ~len:(length s - 2))
let clean_opt = function "NULL" -> None | s -> Option.some @@ clean s

let clean_ts s =
  (* Parses a timestamp with current standard functions..*)
  let s = clean s in
  let s = s ^ "Z" in
  (* make it proper rfc3339 *)
  Ptime.of_rfc3339 ~strict:false s |> Ptime.rfc3339_string_error |> function
  | Ok t -> t
  | Error msg -> failwith @@ "Parsing " ^ s ^ " failed with " ^ msg

(* Read and parse data *)
let read_event_sql_dump file =
  let read_data ic =
    let lines = Seq.of_dispenser @@ fun () -> In_channel.input_line ic in
    let filter_data line =
      String.starts_with ~prefix:"INSERT INTO" line == false
    in
    let read_event_log_data line =
      Scanf.sscanf line "(%d, %d, %s@, %s@, %s@, %s@)"
        (fun id user_id timestamp param value session_id ->
          let param = String.(sub param ~pos:1 ~len:(length param - 2)) in
          let param = event_param_of param in
          let value = clean_opt value in
          let session_id = clean_opt session_id in
          let timestamp, _opt, _opt2 = clean_ts timestamp in
          let new_event = false in
          { id; user_id; timestamp; param; value; session_id; new_event })
    in

    (* Filter events we dont care about *)
    let whitelist_phases event =
      match event.param with
      | Start_workout -> true
      | Complete_workout -> true
      | Phase_progression _ -> true
      | ReadTip -> true
      | PlayVideo -> true
      | Other _str -> false
    in
    let after_cutoff e =
      let cutoff_date = Ptime.of_date (2023, 06, 01) |> Option.get in
      Ptime.is_later e.timestamp ~than:cutoff_date
    in
    let whitelist e = after_cutoff e && whitelist_phases e in

    let event_log_data =
      lines |> Seq.filter filter_data
      |> Seq.map read_event_log_data
      |> Seq.filter whitelist
    in
    Format.eprintf "Reading event data \n";
    (* lazy sequence materalized as an array*)
    event_log_data |> Array.of_seq
  in

  In_channel.with_open_text file read_data

(* Main function *)

let () =
  Printexc.record_backtrace true;
  let file = "../event-logs" in
  let event_log_data = read_event_sql_dump file in

  Format.eprintf "Sorting event log by user id\n";
  Array.sort ~cmp:(fun a b -> a.user_id - b.user_id) event_log_data;

  let event_log_grouped_by_user =
    Array.to_seq event_log_data |> Seq.group (fun a b -> a.user_id == b.user_id)
  in
  Format.eprintf "Verifying and adding missing events\n";

  let print_data group =
    let user_id = (List.hd group).user_id in
    let f e =
      let pp = Ptime.pp_human ~frac_s:6 () in
      if e.new_event then Format.printf "NEW" else ();
      let value = Option.value e.value ~default:"NULL" in
      let session_id = Option.value e.session_id ~default:"NULL" in
      Format.printf "\t%a, %s, %s, %s\n" pp e.timestamp
        (event_param_to_string e.param)
        value session_id
    in
    Format.printf "\nGROUP_user %d\n " user_id;
    List.iter group ~f
  in

  let print_sql groups =
    let user_ids = IntSet.empty  in
    let event_logs = List.concat groups in

    (* let _res =  List.fold_left ~init fixed_groups ~f:print_sql in *)
    Format.printf
      {|INSERT INTO "public"."event_logs" ("user_id", "timestamp", "param", "value") VALUES@.|};
    let new_events = List.filter ~f:(fun e -> e.new_event == true) event_logs in
    let last_idx = List.length new_events - 1 in
    (* let init = (user_ids, 0) in *)
    (* let user_ids = List.fold_left ~init:IntSet.empty ~f:(fun ids e -> IntSet.add e.user_id ids )  new_events in *)

    List.iteri new_events ~f:(fun idx e ->
        (* TODO gather user_id to keep sql *)
        (* row_id => database should generate
           user_id
        *)
        (* 139559, '2023-09-28 23:31:41.235368', 'updateFCMtoken', '{}', NULL), *)
        (* let value *)
        let user_id = e.user_id in
        let pp_timestamp = Ptime.pp_rfc3339 ~frac_s:6 () in
        let param = event_param_to_string e.param in
        Format.printf "(%d,'%a','%s', NULL)" user_id pp_timestamp e.timestamp
          param;

        if last_idx == idx then Format.printf ";@." else Format.printf ",@."
    )
  in

  let process_data () =
    let process_group user_group =
      let group = List.of_seq user_group in
      let group =
        List.sort ~cmp:(fun a b -> Ptime.compare a.timestamp b.timestamp) group
      in
      let group = EventValidate.start group |> List.rev in
      group
      (* dump_data group *)
    in
    let fixed_groups =
      Seq.map process_group event_log_grouped_by_user |> List.of_seq
    in

    (* Dump data for debug *)
    (* let () = List.iter ~f:print_data fixed_groups in *)
    print_sql fixed_groups;

    ()
  in

  process_data ()
