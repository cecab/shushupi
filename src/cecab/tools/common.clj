(ns cecab.tools.common
  (:require [cognitect.transit :as transit]
            [datomic.api :as da]))
(import [java.io ByteArrayInputStream ByteArrayOutputStream])


(defn read-edn
  "A helper function, parse the input-stream a CLJ code."
  [input-stream]
  (some-> input-stream slurp read-string))

(defn decode-transit
  "Decode from transit format."
  [in]
  (let [reader (transit/reader in :json)]
    (transit/read reader)))

(defn encode-transit
  "Encode clj-value as transit format."
  [clj-value]
  (let [out (ByteArrayOutputStream. 4096)
        writer (transit/writer out :json)
        _ (transit/write writer clj-value)]
    (.toString out)))

(defn get-datoms-from-tx
  "Collect the datoms from a tx taking the history from db"
  [db tx]
  (da/q '[:find ?e ?attr ?v ?added
          :in $ ?tx
          :where
          [?e ?a ?v ?tx ?added]
          [?a :db/ident ?attr ]
          [?tx :db/txInstant ?cuando]]
        (da/history db)
        tx))
