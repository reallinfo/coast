(ns coast.db.datahike
  (:require [datahike.api :as d]
            [datahike.migrate :as migrate]))

(def uri "datahike:file:///Users/swlkr/Projects/coast/coast/.db")

(def schema {:member/email {:db/cardinality :db.cardinality/one
                            :db/unique :db.unique/identity}

             :member/name {:db/cardinality :db.cardinality/one
                           :db.unique :db.unique/identity}

             :member/todos {:db/cardinality :db.cardinality/many
                            :db/valueType :db.type/ref}

             :todo/name {:db/cardinality :db.cardinality/one}

             :todo/member {:db/cardinality :db.cardinality/one
                           :db/valueType :db.type/ref}})

(defn migrate-schema [schema]
  (let [conn (d/connect uri)]
    (migrate/export-db @conn "/Users/swlkr/Projects/coast/coast/.db-backup")
    (d/delete-database uri)
    (d/create-database-with-schema uri schema)
    (migrate/import-db conn "/Users/swlkr/Projects/coast/coast/.db-backup")))

(def conn (d/connect uri))

; (comment
;   ;; (re)create database
;   (migrate/export-db @conn "/Users/swlkr/Projects/coast/coast/.db-backup")
;   (d/delete-database uri)
;   (d/create-database-with-schema uri schema)
;   (migrate/import-db conn "/Users/swlkr/Projects/coast/coast/.db-backup"))


; (d/transact conn [{:db/id               (d/tempid -1)
;                    :member/email "swlkr@fastmail.com"
;                    :member/name "swlkr"}
;                   {:db/id               (d/tempid -1)
;                    :member/email "asanborn91@gmail.com"
;                    :member/name "alisha"}])

; (d/transact conn [{:db/id (d/tempid -1)
;                    :todo/name "todo #1"
;                    :todo/member [:member/name "swlkr"]}
;                   {:db/id (d/tempid -1)
;                    :todo/name "todo #2"
;                    :todo/member [:member/name "swlkr"]}])

(d/transact conn [{:db/id 1
                   :member/todos [3 4]}])

(d/fetch [:member/name "swlkr"])

(d/q '[:find ?e ?on
       :where
       [?e :member/todos ?on]]
     @conn)

(d/pull @conn '[:member/email {:member/todos [:todo/name]}]
              [:member/name "swlkr"])
