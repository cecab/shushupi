(ns cecab.tools.shushupi
  (:require
   [datomic.client.api :as d]
   [cognitect.transit :as transit]
   [cecab.tools.common :as common]
   [cecab.tools.wari :as wari]
   [datomic.ion.lambda.api-gateway :as apigw]))
(import [java.io ByteArrayInputStream ByteArrayOutputStream])






(defn initialize-new-ion-db
  "Create the  database ION db-name.Then, transact the attributes
   for migration logging. The attributes are created with a time
   defined by migration-date"
  [db-name migration-date]
  {:new-db 
   (d/create-database (common/get-client) {:db-name db-name})
   :init-transact
   (d/transact
    (common/get-conn-ion db-name)
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

(defn fn-apply-tx
  "Web handler that apply a transaction taken the date and datoms from 
   body"
  [{:keys [headers body]}]
  (let [{:keys [db-name map-datatypes tx-datoms] :as input-data}
        (some-> body common/read-edn)]
    {:status 200
     :headers {"Content-Type" "application/edn"} 
     :body (->
            (wari/apply-tx input-data)
            common/encode-transit)}))

(def init-db
  "API Gateway web service ion for fn-init-db"
  (apigw/ionize fn-init-db))

(def apply-tx
  "API Gateway web service ion for fn-apply-tx"
  (apigw/ionize fn-apply-tx))

