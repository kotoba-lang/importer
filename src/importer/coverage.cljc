(ns importer.coverage
  "What the corpus can and cannot answer for, attached to every answer it
  gives.

  This is the namespace the whole design is for.

  A bot asks `did the customer reply?` and gets back an empty list. That empty
  list has two readings -- `there is no reply` and `we never imported this
  mailbox` -- and the bot will act on them identically. One of those actions is
  wrong, the mistake is invisible in every log, and no amount of care in the
  bot's prompt can recover a distinction the data never carried.

  So a result is not a list. A result is a list plus the coverage that produced
  it, and `answer` is the only constructor. Absence of rows is only evidence of
  absence where coverage says the stream was read.

  It is the same rule `concept-lookup` follows when it prints how many
  repositories are unindexed, and the same one ADR-2608136000 generalised: an
  index that cannot say what it missed reports `not found` for both."
  (:require [importer.cursor :as cur]))

(def states
  "  :covered        read, current, and following a delta
  :backfilling    being read now; older records may not be in yet
  :stale          was read, and the cursor has lapsed or last failed
  :never-imported no cursor has ever advanced for this stream
  :unknown        cannot tell -- the stream declares :unknown retention"
  #{:covered :backfilling :stale :never-imported :unknown})

(def coverage {})

(defn record
  "Fold one cursor and the outcome of its last run into the coverage map."
  [cov c outcome now]
  (assoc cov (cur/stream-key c)
         {:importer.coverage/cursor c
          :importer.coverage/outcome outcome
          :importer.coverage/at now}))

(defn state
  "The three-plus-two valued verdict for one stream key."
  [cov stream-key now]
  (if-let [{:keys [importer.coverage/cursor importer.coverage/outcome]} (get cov stream-key)]
    (let [expired (cur/expired? cursor now)]
      (cond
        (not (cur/started? cursor)) :never-imported
        (= true expired) :stale
        (nil? expired) :unknown
        (not= :synced outcome) :stale
        (= :backfill (cur/phase cursor)) :backfilling
        :else :covered))
    :never-imported))

(defn through
  "The watermark this stream is read through, or nil."
  [cov stream-key]
  (some-> (get cov stream-key) :importer.coverage/cursor cur/watermark))

(defn answer
  "The only way to build a corpus result.

  Rows alone are not a result; there is deliberately no arity that takes them.
  `stream-keys` are the streams the query drew from -- naming them is what lets
  a caller see that its question ranged over a mailbox nobody has imported."
  [rows cov stream-keys now]
  (let [per (into {}
                  (map (fn [k]
                         [k {:importer.coverage/state (state cov k now)
                             :importer.coverage/through (through cov k)}]))
                  stream-keys)]
    {:importer/rows (vec rows)
     :importer/coverage per
     :importer/complete? (every? #(= :covered (:importer.coverage/state %)) (vals per))
     :importer/at now}))

(defn complete? [a] (boolean (:importer/complete? a)))

(defn gaps
  "The stream keys whose state is anything but :covered, with their state. What
  a caller prints next to an empty list so a reader is not left to assume."
  [a]
  (->> (:importer/coverage a)
       (remove #(= :covered (:importer.coverage/state (val %))))
       (map (fn [[k v]] [k (:importer.coverage/state v)]))
       (sort-by (comp str first))
       vec))

(defn empty-and-incomplete?
  "No rows, and at least one stream was not fully read.

  The single condition under which an answer must not be read as `no`. Worth a
  predicate rather than a comment, because it is the one a caller forgets."
  [a]
  (and (empty? (:importer/rows a)) (not (complete? a))))
