(ns cecab.tools.boa
  (:require [org.httpkit.client :as http]
            [datomic.api :as da]
            [datomic.client.api :as d]
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

(defn get-map-datatypes
  "Extract the idents for numerical entities that are 
     referenced in the tuples of los-datoms"
  [pro-db los-datoms]
  (reduce
   (fn [acc [x-eid x-attr eid added?]]
     (if-let [i (:db/ident
                 (da/pull pro-db [:db/ident] eid))]
       (assoc acc eid i)
       acc))
   {}
   los-datoms))

(defn apply-tx
  "Build a transaction from los-datoms and apply it as the original 
   datetime to db-name Cloud DB"
  [{:keys [db-name los-datoms  map-datatypes] :as all-data}]
  (let [
        ion-db  (d/db (common/get-conn-ion db-name))
        los-tipos
        (reduce
         (fn [acc [e t]]
           (assoc acc e t))
         {}
         (d/q '[:find ?e ?tipo
                :in $ [?e ...]
                :where
                [?e :db/valueType ?vt]
                [?vt :db/ident ?tipo]]
              ion-db
              (into #{} (filter #(not= :db/index %)
                                (map #(get % 1) los-datoms)))))
        las-refs-attribs
        (map first
             (filter
              (fn [[k v]]
                (= :db.type/ref v))
              los-tipos))
        ;; Debemos separar a la misma TX, de las otras entidades.
        eid-tx
        (ffirst
         (filter #(= :db/txInstant (get % 1)) los-datoms))
        audit-user
        (->
         (filter #(and  (= :audit/user (get % 1))
                        (= eid-tx (first %1))) los-datoms) first (get 2))
        eid-entities
        (reduce merge {}
                (map-indexed
                 (fn [i v]
                   {(first v) i})
                 (filter #(not= eid-tx (first %)) los-datoms)))
        reverse-temp-entities
        (reduce
         (fn [acc [k v]]
           (assoc acc (str v) k))
         {}
         eid-entities)
        set-ref-eids
        (into #{}
              (map
               #(get % 2)
               (filter
                #(contains? (set las-refs-attribs) (get % 1))
                los-datoms)))
        migrated-eids
        (reduce
         (fn [acc [k v]]
           (assoc acc k v)) {}
         (d/q '[:find ?pro-eid ?ion-eid
                :in $ [?pro-eid ...]
                :where
                [?ion-eid :pifaho/orig-eid ?pro-eid]]
              ion-db
              (clojure.set/union (set (keys eid-entities))
                                 set-ref-eids)))
        map-existe?
        (reduce
         (fn [acc eid]
           (assoc acc eid (contains? (set (keys  migrated-eids)) eid)))
         {}
         (keys eid-entities))
        ;;
        las-new-entities
        (reduce
         (fn [acc tupla]
           (let [[the-eid the-key the-value added] tupla
                 x-value
                 (cond
                   ;; If is an entity that has an :db/ident
                   (contains? #{:db/valueType :db/cardinality
                                :db/unique }  the-key)
                   (get map-datatypes the-value)
                   ;; If the-key is of db.type REF, need translation to
                   ;; Ion Database. But if it isn't found there, take it
                   ;; from the TEMP-IDs inside this transaction.
                   (= :db.type/ref (get los-tipos the-key))
                   (get migrated-eids the-value
                        (str  (get eid-entities the-value)))
                   :else
                   the-value)]
             ;; Some invariants..
             (assert (not (nil? x-value)))
             (cond
               ;; Es una operacion de TRANSACTION 
               (and  (contains? (into #{} (keys  eid-entities)) the-eid)
                     (last tupla)
                     (not= 0 the-eid)
                     (not= :db/index the-key))
               (assoc-in acc [:added
                              (get migrated-eids the-eid
                                   (str  (get eid-entities the-eid)))
                              the-key]
                         x-value)
               ;; Cuando la transaccion agrega un ATTRIB, modifica via
               ;; :db.install/attribute
               (and  (= 0 the-eid) (= :db.install/attribute the-key)
                     (not (map-existe? the-value)))
               (assoc-in acc [:added
                              (get migrated-eids the-value
                                   (str  (get eid-entities the-value)))
                              :db.install/_attribute]
                         :db.part/db)
               ;; Para las RETRACTIONS.
               (and  (contains? (into #{} (keys  eid-entities)) the-eid)
                     (not  added))
               (update-in acc [:retract ] conj
                          [:db/retract (get migrated-eids the-eid)
                           the-key x-value])
               :else
               acc)))
         {:added {} :retract []}
         los-datoms)
        ;;---
        new-tx
        (update-in las-new-entities
                   [:added]
                   (fn [map-news]
                     (mapv
                      (fn [[eid-orig map-values]]
                        (if (string? eid-orig)
                          (-> map-values
                              (assoc :pifaho/orig-eid
                                     (get reverse-temp-entities eid-orig))
                              (assoc  :pifaho/orig-tx eid-tx)
                              (assoc :db/id eid-orig))
                          (assoc map-values :db/id eid-orig)))
                      map-news)))
        ;; ---
        new-tx-ion
        (reduce
         (fn [acc [tx-eid tx-attrib tx-value]]
           (assoc acc tx-attrib tx-value))
         {:db/id "datomic.tx"}
         (filter #(= eid-tx (first %)) los-datoms))]
    (d/transact
     (common/get-conn-ion db-name)
     {:tx-data (vec
                (concat
                 [new-tx-ion]
                 (concat (:added new-tx) (:retract new-tx))))})))

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


  ;;; ==========================================================
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
  (def the-datoms (common/get-datoms-from-tx pro-db  (first (rest all-txs))))
  
  ;; Sent a request.
  (def mig-1
    {:tx-datoms the-datoms
     :map-datatypes (get-map-datatypes the-datoms)})
  (def url-apigateway-apply-tx
    "https://3lynibgz02.execute-api.us-east-2.amazonaws.com/dev/datomic")
  ;; --- 
  (def migrator-1 (post-request url-apigateway-apply-tx mig-1))
  ;; 
  (common/get-datoms-from-tx pro-db (first tx-attribs))

  ;; --- future MIGRATE-TX
  (def los-datoms the-datoms)
  
  
  (def map-datatypes (get-map-datatypes los-datoms))
  
  
  
  


  )
