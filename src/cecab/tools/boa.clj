(ns cecab.tools.boa
  (:require [org.httpkit.client :as http]
            [cecab.tools.common :as common]))

(defn post-request
  "Post a Request to API/Gateway"
  [url value]
  (let [
        {:keys [status headers body error] :as resp}
        @(http/post
          url
          {:body
           (str value)})
        clj-content (common/decode-transit body)]
    clj-content))

(comment
  ;; ---
  (def db-name "ion-19")
  (def migration-date #inst "2017-01-02T00:00:00.000-00:00")
  ;; Inicio de la base de datos.. 
  (def post-value {:db-name db-name :migration-date migration-date})
  (def url-apigateway-init-db
    "https://h3ma1b87y7.execute-api.us-east-2.amazonaws.com/dev/datomic")
  ;; 1er. Request..
  (def out-x (post-as-transit url-apigateway-init-db post-value))
  ;; Conseguir las tx.
  
  


  )
