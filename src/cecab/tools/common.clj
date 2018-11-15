(ns cecab.tools.common
  (:require [cognitect.transit :as transit]
            [datomic.api :as da]
            [datomic.client.api :as d]))
(import [java.io ByteArrayInputStream ByteArrayOutputStream])


(def get-client
  "This function will return a local implementation of the client
  interface when run on a Datomic compute node. If you want to call
  locally, fill in the correct values in the map."
  (memoize #(d/client {:server-type :ion
                       :region "us-east-2"
                       :system "cloudker"
                       :query-group "cloudker"
                       :endpoint "http://entry.cloudker.us-east-2.datomic.net:8182"
                       :proxy-port 8182})))

(defn get-conn-ion
  "Returns a connection to the ION db-name"
  [db-name]
  (d/connect (get-client) {:db-name db-name}))


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
