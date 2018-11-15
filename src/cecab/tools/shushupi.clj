(ns cecab.tools.shushupi
  (:require
   [datomic.client.api :as d]
   [cognitect.transit :as transit]
   [cecab.tools.common :as common]
   [datomic.ion.lambda.api-gateway :as apigw]))
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

(defn initialize-new-ion-db
  "Create the  database ION db-name. Transact the attributes
   for migration logging. The attributes are create with a time
   defined by migration-date"
  [db-name migration-date]
  {:new-db 
   (d/create-database (get-client) {:db-name db-name})
   :init-transact
   (d/transact
    (get-conn-ion db-name)
    {:tx-data
     [{:db/id "datomic.tx"
       :db/txInstant migration-date}
      {;; The on-premise entity ID. 
       :db/ident :pifaho/orig-eid
       :db/valueType :db.type/long
       :db/cardinality :db.cardinality/one
       :db/unique :db.unique/identity}
      ;; The on-premite transaction 
      {:db/ident :pifaho/orig-tx
       :db/valueType :db.type/long
       :db/cardinality :db.cardinality/one}]})})







(defn fn-init-db
  "Web handler that create the database prior to migration."
  [{:keys [headers body]}]
  (let [{:keys [db-name migration-date] :as input-data} (some-> body common/read-edn)]
    {:status 200
     :headers {"Content-Type" "application/edn"} 
     :body (-> 
            (initialize-new-ion-db db-name migration-date)
            :new-db common/encode-transit)}))
(def init-db
  "API Gateway web service ion for fn-init-db"
  (apigw/ionize fn-init-db))

(comment
  ;; --
  (get-client)
  )
