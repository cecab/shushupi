(ns cecab.tools.wari
  (:require [datomic.client.api :as d]
            [cecab.tools.common :as common]))


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
