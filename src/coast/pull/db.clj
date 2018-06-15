(ns coast.pull.db
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as string]
            [clojure.set]
            [coast.pull.sql :as pull.sql]
            [coast.pull.pull]
            [coast.pull.fmt]
            [coast.time :as time])
  (:refer-clojure :exclude [drop update]))

(defn sql-vec? [v]
  (and (vector? v)
       (string? (first v))
       (not (string/blank? (first v)))))

(defn query [conn sql-vec]
  (if (sql-vec? sql-vec)
    (jdbc/query conn sql-vec {:keywordize? false})
    '()))

(defn validate-map [schema m]
  (let [ident-ks (clojure.set/intersection (-> m keys set)
                                           (:idents schema))
        col-ks (clojure.set/intersection (-> m keys set)
                                         (:cols schema))
        join-ks (clojure.set/intersection (-> m keys set)
                                          (->> (vals schema)
                                               (filter map?)
                                               (map :db/joins)
                                               (filter some?)
                                               (set)))]
    (merge (select-keys m col-ks) (select-keys m ident-ks)
           (select-keys m join-ks))))

(defn id [conn schema ident]
  (let [sql-vec (pull.sql/id schema ident)
        row (-> (query conn sql-vec)
                (first))]
    (get row "id")))

(defn identify-kv [conn schema [k v]]
  (if (pull.sql/ident? schema v)
    [(keyword (namespace k) (str (name k) "_id")) (id conn schema v)]
    [k v]))

(defn identify-map [conn schema m]
  (->> (map (fn [[k v]] (identify-kv conn schema [k v])) m)
       (into (empty m))))

(defn qualify-map [k-ns m]
  (->> (map (fn [[k v]] [(keyword k-ns (name k)) v]) m)
       (into (empty m))))

(defn single [coll]
  (if (and (= 1 (count coll))
           (coll? coll))
    (first coll)
    coll))

(defn insert [conn schema val]
  (jdbc/with-db-connection [db-conn conn]
    (jdbc/with-db-transaction [db-tran db-conn]
      (let [v (if (map? val) [val] val)
            v (map #(validate-map schema %) v)
            v (map #(assoc % (keyword (-> v first keys first namespace) "updated-at") (time/now)) v)
            v (map #(identify-map db-tran schema %) v)
            sql-vec (pull.sql/insert schema v)
            rows (query db-tran sql-vec)]
        (->> (map #(qualify-map (-> v first keys first namespace) %) rows)
             (single))))))

(defn fetch [conn schema ident]
  (jdbc/with-db-connection [db-conn conn]
    (jdbc/with-db-transaction [db-tran db-conn]
      (let [sql-vec (pull.sql/fetch schema ident)
            row (first (query db-tran sql-vec))]
        (qualify-map (-> ident first namespace) row)))))

(defn update [conn schema m ident]
  (jdbc/with-db-connection [db-conn conn]
    (jdbc/with-db-transaction [db-tran db-conn]
      (let [k-ns (-> m keys first namespace)
            m (assoc m (keyword k-ns "updated-at") (time/now))
            sql-vec (pull.sql/update schema m ident)
            rows (query db-tran sql-vec)]
        (map #(qualify-map (-> ident first namespace) %) rows)))))

(defn delete [conn schema ident]
  (jdbc/with-db-connection [db-conn conn]
    (jdbc/with-db-transaction [db-tran db-conn]
      (let [sql-vec (pull.sql/delete schema ident)
            row (-> (query db-tran sql-vec)
                    (first))]
        (qualify-map (-> ident first namespace) row)))))

(defn pull [conn schema v ident]
  (let [cols (filter keyword? v)
        rels (filter map? v)
        col-vec (if (empty? cols)
                  []
                  (coast.pull.pull/sql-vec schema cols ident))
        rel-vecs (map #(coast.pull.pull/sql-vec schema [%] ident) rels)
        vecs (->> (concat [col-vec] rel-vecs)
                  (filter #(not (empty? %))))]
    (jdbc/with-db-connection [db-conn conn]
      (jdbc/with-db-transaction [db-tran db-conn]
        (let [rows (map #(query db-tran %) vecs)
              results (apply concat rows)]
          (coast.pull.fmt/fmt-pull-rows results))))))
