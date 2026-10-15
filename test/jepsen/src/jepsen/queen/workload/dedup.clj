(ns jepsen.queen.workload.dedup
  "W6 --workload dedup: transactionId dedup across failover.

    :send [p id n]  one-item push of payload {id, n} to partition d<p> of the
                    first test queue with transactionId id. ids come from a
                    small pool per partition, so most sends reuse an id. When
                    the answer is unknown (timeout, 5xx, broken connection) the
                    SAME push is sent again once, to ANOTHER node.
                    :ok with the answer's status (queued | duplicate) and
                    offset.
    :final-read     every record of every d<p> partition, from offset 0.

  With --batch-sends N the sends are instead
    :send-batch [p [[id n] ...]]  ONE push of 2 to N items, distinct ids of
                    partition d<p>: one push command of one partition, whose
                    outcome the broker writes as runs (catalogue version 7).
                    An unknown answer is retried once, the same push, on
                    another node. :ok with each item's status and offset.
  The checker judges each item of a :send-batch as the :send it stands for.

  Checker, against the last final read:
    - at most one record per (partition, id);
    - every :ok answer's offset holds a record with that id (a duplicate
      answer names the original's offset);
    - every id with an :ok answer has its record;
    - at most one :ok answer per (partition, id) says queued, and the stored
      record carries that send's n.
  The queues are configured with dedupWindowSeconds 86400."
  (:require [clojure.tools.logging :refer [info warn]]
            [jepsen [checker :as checker]
                    [client :as client]
                    [generator :as gen]
                    [history :as h]
                    [random :as rand]]
            [jepsen.queen [http :as qh]
                          [kv :as kv]]
            [jepsen.queen.workload [log :as log]]))

(defn queue-name [test] (first (:queue-names test)))
(defn partitions [test] (:w6-partitions test 4))
(defn id-pool [test] (:w6-ids test 40))

(defn- push-once
  "One push via base: {:ok status offset} | {:type t :error e}."
  [client test base p id n]
  (let [r (qh/request! (:http client) :post (str base "/api/v1/push")
                       {:items [{:queue         (queue-name test)
                                 :partition     (str "d" p)
                                 :payload       {:id id, :n n}
                                 :transactionId id}]}
                       (:client-timeout-ms test))]
    (if (= 201 (:status r))
      (let [item (first (:body r))]
        (if (#{"queued" "duplicate"} (:status item))
          {:ok true, :status (:status item), :offset (:offset item)}
          {:type :fail, :error [:item-error item]}))
      {:type  (log/push-failure test (:status r) (:body r))
       :error [(:status r) (:body r)]})))

(defn- guarded
  "push-once, with a transport failure turned into an answer."
  [client test base p id n]
  (try (push-once client test base p id n)
       (catch clojure.lang.ExceptionInfo e
         (let [{:keys [type msg]} (ex-data e)]
           (if (#{::qh/refused ::qh/timeout ::qh/io} type)
             {:type (kv/exception-type type), :error [(keyword (name type)) msg]}
             (throw e))))))

(defn- send!
  [client test op]
  (let [[p id n] (:value op)
        first-r  (guarded client test (:base client) p id n)
        [r retry] (if (= :info (:type first-r))
                    (let [other (rand/nth (vec (remove #{(:node client)} (:nodes test))))]
                      [(guarded client test (qh/base-url other) p id n) other])
                    [first-r nil])]
    (cond
      (:ok r)  (assoc op :type :ok, :status (:status r), :offset (:offset r)
                      :retried-on retry :first-error (when retry (:error first-r)))
      ; A retry that failed definitely does not undo the unknown first try.
      retry    (assoc op :type :info, :error [:after-retry (:error first-r) (:error r)])
      :else    (assoc op :type (:type r), :error (:error r)))))

(defn batch-sends [test] (long (:batch-sends test 0)))

(defn- push-batch-once
  "One push of items [[id n] ...] to partition d<p> via base:
  {:ok true, :statuses [...], :offsets [...]} | {:type t :error e}. A refusal
  refuses a whole push group, so an answer that mixes refused and accepted
  items is not expected: :info."
  [client test base p items]
  (let [r (qh/request! (:http client) :post (str base "/api/v1/push")
                       {:items (mapv (fn [[id n]]
                                       {:queue         (queue-name test)
                                        :partition     (str "d" p)
                                        :payload       {:id id, :n n}
                                        :transactionId id})
                                     items)}
                       (:client-timeout-ms test))]
    (if (= 201 (:status r))
      (let [answers (vec (:body r))
            ok?     (fn [a] (contains? #{"queued" "duplicate"} (:status a)))
            all?    (= (count answers) (count items))]
        (cond
          (and all? (every? ok? answers))
          {:ok true, :statuses (mapv :status answers), :offsets (mapv :offset answers)}

          (and all? (not-any? ok? answers))
          {:type :fail, :error [:item-errors (vec (distinct answers))]}

          :else
          {:type :info, :error [:mixed-item-answers answers]}))
      {:type  (log/push-failure test (:status r) (:body r))
       :error [(:status r) (:body r)]})))

(defn- guarded-batch
  "push-batch-once, with a transport failure turned into an answer."
  [client test base p items]
  (try (push-batch-once client test base p items)
       (catch clojure.lang.ExceptionInfo e
         (let [{:keys [type msg]} (ex-data e)]
           (if (#{::qh/refused ::qh/timeout ::qh/io} type)
             {:type (kv/exception-type type), :error [(keyword (name type)) msg]}
             (throw e))))))

(defn- send-batch!
  [client test op]
  (let [[p items] (:value op)
        first-r   (guarded-batch client test (:base client) p items)
        [r retry] (if (= :info (:type first-r))
                    (let [others (vec (remove #{(:node client)} (:nodes test)))
                          other  (if (seq others) (rand/nth others) (:node client))]
                      [(guarded-batch client test (qh/base-url other) p items) other])
                    [first-r nil])]
    (cond
      (:ok r)  (assoc op :type :ok, :statuses (:statuses r), :offsets (:offsets r)
                      :retried-on retry :first-error (when retry (:error first-r)))
      retry    (assoc op :type :info, :error [:after-retry (:error first-r) (:error r)])
      :else    (assoc op :type (:type r), :error (:error r)))))

(defn- distinct-ids
  "k distinct ids of partition p's pool (k at most the pool's size)."
  [test p k]
  (let [k (min k (id-pool test))]
    (loop [picked #{}, out []]
      (if (= k (count out))
        out
        (let [i (rand/long (id-pool test))]
          (if (picked i)
            (recur picked out)
            (recur (conj picked i) (conj out (str "d" p "-" i)))))))))

(defn- item-sends
  "The :ok sends the checker judges: a :send as it is, a :send-batch as one
  :send per item, with that item's status and offset."
  [ops]
  (mapcat (fn [op]
            (if (= :send-batch (:f op))
              (let [[p items] (:value op)]
                (map (fn [[id n] status offset]
                       (assoc op :value [p id n], :status status, :offset offset))
                     items (:statuses op) (:offsets op)))
              [op]))
          ops))

(defn- fetch-partition
  [client test partition]
  (loop [off 0, out []]
    (let [r (qh/request! (:http client) :post (str (:base client) "/api/v1/fetch")
                         {:entries [{:queue (queue-name test), :partition partition, :offset off}]}
                         (:client-timeout-ms test))
          e (first (:entries (:body r)))]
      (cond
        (not= 200 (:status r)) (throw (ex-info "fetch failed" {:status (:status r)}))
        (seq (:records e))     (recur (inc (long (:offset (peek (:records e))))) (into out (:records e)))
        :else                  out))))

(defn- final-read!
  [client test op]
  (assoc op :type :ok
         :value (->> (range (partitions test))
                     (mapcat (fn [p]
                               (map (fn [r] {:partition p, :offset (:offset r)
                                             :id (:transactionId r), :n (:n (:payload r))})
                                    (fetch-partition client test (str "d" p)))))
                     vec)))

(defrecord Client [node base http]
  client/Client
  (open! [this test node]
    (assoc this :node node, :base (qh/base-url node), :http (qh/client)))

  (setup! [this test])

  (invoke! [this test op]
    (try
      (case (:f op)
        :send       (send! this test op)
        :send-batch (send-batch! this test op)
        :final-read (final-read! this test op))
      (catch clojure.lang.ExceptionInfo e
        (let [{:keys [type msg]} (ex-data e)]
          (cond
            (#{::qh/refused ::qh/timeout ::qh/io} type)
            (assoc op :type :fail, :error [(keyword (name type)) msg])

            (= "fetch failed" (.getMessage e))
            (assoc op :type :fail, :error [:fetch (ex-data e)])

            :else (throw e))))))

  (teardown! [this test])

  (close! [this test])

  client/Reusable
  (reusable? [this test] true))

(defn client
  []
  (map->Client {}))

(defn checker
  []
  (reify checker/Checker
    (check [this test history opts]
      (let [ops    (h/client-ops history)
            finals (->> ops (h/filter #(and (= :final-read (:f %)) (= :ok (:type %)))) vec)]
        (if (empty? finals)
          {:valid? :unknown, :error "no final read succeeded"}
          (let [recs    (:value (apply max-key #(:time (h/invocation history %)) finals))
                by-pid  (group-by (juxt :partition :id) recs)
                at      (into {} (map (juxt (juxt :partition :offset) identity)) recs)
                sends   (->> ops
                             (h/filter #(and (#{:send :send-batch} (:f %)) (= :ok (:type %))))
                             item-sends
                             vec)
                dups    (->> by-pid (filter #(< 1 (count (val %))))
                             (map (fn [[[p id] rs]] {:partition p, :id id
                                                     :offsets (mapv :offset rs)}))
                             vec)
                wrong   (->> sends
                             (keep (fn [op]
                                     (let [[p id n] (:value op)
                                           rec (at [p (:offset op)])]
                                       (when (not= id (:id rec))
                                         {:op (:index op), :partition p, :id id
                                          :status (:status op), :offset (:offset op)
                                          :record-there rec}))))
                             vec)
                lost    (->> sends
                             (map (fn [op] (let [[p id] (:value op)] [p id])))
                             distinct
                             (remove by-pid)
                             vec)
                queued  (->> sends (filter #(= "queued" (:status %)))
                             (group-by (fn [op] (let [[p id] (:value op)] [p id]))))
                two-q   (->> queued (filter #(< 1 (count (val %))))
                             (map (fn [[k ops]] {:at k, :ns (mapv (comp #(nth % 2) :value) ops)}))
                             vec)
                bad-n   (->> queued
                             (keep (fn [[k ops]]
                                     (let [stored (:n (first (by-pid k)))
                                           ns     (set (map (comp #(nth % 2) :value) ops))]
                                       (when (and stored (not (ns stored)))
                                         {:at k, :stored-n stored, :queued-ns ns}))))
                             vec)]
            {:valid?         (and (empty? dups) (empty? wrong) (empty? lost)
                                  (empty? two-q) (empty? bad-n))
             :records        (count recs)
             :ids            (count by-pid)
             :sends-ok       (count sends)
             :duplicate-answers (count (filter #(= "duplicate" (:status %)) sends))
             :retried        (count (filter :retried-on sends))
             :duplicate-records     (take 16 dups)
             :duplicate-record-count (count dups)
             :wrong-offset   (take 16 wrong)
             :wrong-offset-count (count wrong)
             :lost-ids       (take 16 lost)
             :two-queued     (take 16 two-q)
             :stored-n-not-queued (take 16 bad-n)}))))))

(defn workload
  [opts]
  (let [n (atom -1)]
    {:client          (client)
     :checker         (checker)
     :generator       (fn [test ctx]
                        (let [p (rand/long (partitions test))
                              b (batch-sends test)]
                          (if (pos? b)
                            {:f     :send-batch
                             :value [p (mapv (fn [id] [id (swap! n inc)])
                                             (distinct-ids test p (+ 2 (rand/long (dec b)))))]}
                            {:f     :send
                             :value [p (str "d" p "-" (rand/long (id-pool test)))
                                     (swap! n inc)]})))
     :final-generator (gen/each-thread {:f :final-read, :value nil})}))
