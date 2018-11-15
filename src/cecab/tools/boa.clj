(ns cecab.tools.boa
  (:require [org.httpkit.client :as http]
            [datomic.api :as da]
            [cecab.tools.common :as common]))

(defn post-request
  "Post a Request to API/Gateway identified by url with
   a content value, which is a CLJ value"
  [url value]
  (let [
        {:keys [status headers body error] :as resp}
        @(http/post
          url
          {:body
           (str value)})
        clj-content (common/decode-transit body)]
    clj-content))


(defn get-txs-for-schema
    "Get a vector sorted by tx ID for all the transactions
     that created any custom attribute in db."
    [db]
    (let [epoc  #inst "1970-01-01T00:00:00.000-00:00"]
      (sort
       (da/q '[:find [?tx ...]
               :in $ ?ref
               :where
               [?e :db/valueType ?v ?tx ?added]
               [?e :db/ident ?a ?tx ?added]
               [?tx :db/txInstant ?when]
               [(> ?when ?ref)]]
             (da/history db)
             epoc))))

(defn get-first-custom-tx-schema
    "Find the first transaction for the schema in db"
    [db]
    (:db/txInstant
     (da/pull db '[:db/txInstant]
              (-> db get-txs-for-schema first))))


(defn get-schema-attribs
    "Get a list of all attributes in pro-db taken from the history 
     of the database"
    [pro-db]
    (da/q '[:find [?v ...]
            :in $ [?tx ...]
            :where
            [?e :db/ident ?v ?tx ?added]]
          (da/history pro-db)
          (get-txs-for-schema pro-db)))

(defn get-all-tx-data
    "It give a list of all the transactions excepto those
     that created attributes in the schema of pro-db"
    [pro-db]
    (sort
     (da/q '[:find [?tx ...]
             :in $ ?ref  [?a ...]
             :where
             [?e ?a ?v ?tx ?added]
             [?tx :db/txInstant ?when]
             [(>= ?when ?ref)]]
           (da/history pro-db)
           (get-first-custom-tx-schema pro-db)
           (get-schema-attribs pro-db))))
(comment
  ;; ---
  (def db-name "ion-19")
  (def migration-date #inst "2017-01-02T00:00:00.000-00:00")
  ;; Inicio de la base de datos.. 
  (def post-value {:db-name db-name :migration-date migration-date})
  (def url-apigateway-init-db
    "https://h3ma1b87y7.execute-api.us-east-2.amazonaws.com/dev/datomic")
  ;; 1er. Request..
  (def out-x (post-request url-apigateway-init-db post-value))


  
  ;; Conseguir las tx.
  (def pro-uri "datomic:dev://localhost:4334/complejo")
  (def pro-db 
    (da/db (da/connect pro-uri)))
  ;; These atttribs are created by Datomic during its initialitation
  
  
  (def tx-attribs (get-txs-for-schema pro-db))
  (def tx-data (get-all-tx-data pro-db))
  ;; ----
  (def all-txs (concat tx-attribs tx-data))
  
  ;; We have to sent every tx in all-txs IN ORDER to Datomic Ion
  ;; using the API Gateway.
  (def the-datoms (common/get-datoms-from-tx pro-db  (first all-txs)))
  
  ;; Sent a request.
  (def mig-1
    {:tx-datoms the-datoms})
  (def url-apigateway-apply-tx
    "https://3lynibgz02.execute-api.us-east-2.amazonaws.com/dev/datomic")
  ;; --- 
  (def migrator-1 (post-request url-apigateway-apply-tx mig-1))
  (common/get-datoms-from-tx pro-db (first tx-attribs))

  
  
  
  


  )
