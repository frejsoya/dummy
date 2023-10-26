open StdLabels
open MoreLabels

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
  (* TODO: Quick hack as we have no phase > 3*)
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
    }
  in
  Format.eprintf "Creating new event:\n%a\n" pp_event_log new_event;
  new_event

(* event.param ->
   Some param
   None ->
*)
let f s = failwith s

module FSM = struct
  let next_param = function [] -> None | e :: rest -> Some (e.param, e, rest)

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
    | [] -> (
        try
          let previous_event = List.hd state in
          let new_complete = make_new_complete previous_event in
          new_complete :: state
        with Failure _msg ->
          failwith "state phrase progression failed with empty state")
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

let pp_list = Format.pp_print_list pp_event_log
let clean s = String.(sub s ~pos:1 ~len:(length s - 2))
let clean_opt = function "NULL" -> None | s -> Option.some @@ clean s

let clean_ts s =
  let s = clean s in
  let s = s ^ "Z" in
  (* make it proper rfc3339 *)
  Ptime.of_rfc3339 ~strict:false s |> Ptime.rfc3339_string_error |> function
  | Ok t -> t
  | Error msg -> failwith @@ "Parsing " ^ s ^ " failed with " ^ msg

let () =
  Printexc.record_backtrace true;

  print_endline "Hello, World!\n";
  let file = "../event-logs" in

  (* Ineffective, loading all into mem as a linked list.  *)
  let lines = In_channel.with_open_text file In_channel.input_lines in

  let data =
    List.filter
      ~f:(fun line -> String.starts_with ~prefix:"INSERT INTO" line == false)
      lines
  in
  let data =
    List.map data ~f:(fun line ->
        let elem =
          Scanf.sscanf line "(%d, %d, %s@, %s@, %s@, %s@)"
            (fun id user_id timestamp param value session_id ->
              let param = String.(sub param ~pos:1 ~len:(length param - 2)) in
              let param = event_param_of param in
              let value = clean_opt value in
              let session_id = clean_opt session_id in
              let timestamp, _opt, _opt2 = clean_ts timestamp in
              { id; user_id; timestamp; param; value; session_id })
        in
        elem)
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
  let data = List.filter data ~f:whitelist in

  (* Sort and group by user id *)
  let data = List.sort ~cmp:(fun a b -> a.user_id - b.user_id) data in
  let seq = List.to_seq data in
  let group_by_user = Seq.group (fun a b -> a.user_id == b.user_id) seq in

  let () =
    Seq.iter
      (fun user_group ->
        let group = List.of_seq user_group in
        let user_id = (List.hd group).user_id in
        let group =
          List.sort
            ~cmp:(fun a b -> Ptime.compare a.timestamp b.timestamp)
            group
        in
        let fixed_group = FSM.start group |> List.rev in
        Format.printf "\nGROUP_user %d\n " user_id;
        let pp = Ptime.pp_human ~frac_s:6 () in
        List.iter fixed_group ~f:(fun e ->
            Format.printf "\t%a, %s,\n" pp e.timestamp
              (event_param_to_string e.param)))
      group_by_user
  in
  ()

(* Valid sequences
   state machine
   |
    - No_events

   current_workout = start_event, phase_progressions,others,complete_event


   if start_event:
     examine previous state for completion.
*)
let log_error event =
  Format.eprintf "Skippping spurious phase event %a\n" pp_event_log event

(* let start_workout_transition event state = match event.param with
   | Start_workout -> start_workout_transition
   | Complete_workout -> (* has *)
   | Phase_progression _ ->
   | Other s -> failwith "Unknown event: " ^ s
   in *)

(* initial: No_Events
 * initial---start :: xs--->Start ts
   initial---workout :: xs --->Start ts
   initial---phase completion :: xs --> skip;initial
   initial---
 *)

(* type fsm = `Initial | `Start of event_log | `Phases of event_log list *)

(* let pp_list = Format.pp_print_list pp_event_log in *)
(* Format.printf "%a" pp_list data *)
