(ns coast.db
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as string]
            [coast.env :as env]
            [coast.queries :as queries]
            [coast.utils :as utils]
            [coast.sql :as sql]
            [coast.pull.db]
            [coast.schema :as schema])
  (:refer-clojure :exclude [drop update]))

(defn not-null-constraint [s]
  (let [col (-> (re-find #"null value in column \"(\w+)\" violates not-null constraint" s)
                (second))]
    (if (nil? col)
      {}
      {(keyword col) (str (utils/humanize col) " cannot be blank")})))

(defn unique-constraint [s]
  (let [col (-> (re-find #"(?s)duplicate key value violates unique constraint.*Detail: Key \((.*)\)=\((.*)\)" s)
                (second))]
    (if (nil? col)
      {}
      {(keyword col) (str (utils/humanize col) " is already taken")
       :type :unique-constraint-violation})))

(defmacro transact! [f]
  `(try
     ~f
     (catch org.postgresql.util.PSQLException e#
       (let [msg# (.getMessage e#)
             err1# (not-null-constraint msg#)
             err2# (unique-constraint msg#)
             errors# (merge err1# err2#)]
         (if (empty? errors#)
           (throw e#)
           (throw
            (ex-info
             (str "Invalid data: "
                  (string/join " " (vals errors#)))
             {:type :invalid :errors errors#})))))))

(defn connection []
  (let [db-url (or (env/env :database-url)
                   (env/env :db-spec-or-url))]
    (if (string/blank? db-url)
      (throw (Exception. "Your database connection string is blank. Set the DATABASE_URL or DB_SPEC_OR_URL environment variable"))
      {:connection (jdbc/get-connection db-url)})))

(defn admin-connection []
  (let [db-url (or (env/env :admin-db-spec-or-url)
                   (env/env :admin-database-url)
                   "postgres://localhost:5432/postgres")]
    (if (string/blank? db-url)
      (throw (Exception. "Your admin database connection string is blank. Set the ADMIN_DB_SPEC_OR_URL environment variable"))
      {:connection (jdbc/get-connection db-url)})))

(defn execute! [db sql]
  (jdbc/execute! db sql))

(defn exec [db sql]
  (jdbc/with-db-connection [conn db]
    (with-open [s (.createStatement (jdbc/db-connection conn))]
      (.addBatch s sql)
      (seq (.executeBatch s)))))

(defn sql-vec? [v]
  (and (vector? v)
       (string? (first v))
       (not (string/blank? (first v)))))

(defn query
  ([conn v opts]
   (if (and (sql-vec? v) (map? opts))
     (transact!
       (jdbc/with-db-connection [db-conn conn]
         (jdbc/query db-conn v {:keywordize? true
                                :identifiers utils/kebab})))
     '()))
  ([conn v]
   (query conn v {})))

(defn create-root-var [name value]
  ; shamelessly stolen from yesql
  (intern *ns*
          (with-meta (symbol name)
                     (meta value))
          value))

(defn query-fn [{:keys [sql f]}]
  (fn [& [m]]
    (->> (queries/sql-vec sql m)
         (query (connection))
         (f))))

(defn query-fns [filename]
   (doall (->> (queries/slurp-resource filename)
               (queries/parse)
               (map #(assoc % :ns *ns*))
               (map #(create-root-var (:name %) (query-fn %))))))

(defmacro defq
  ([n filename]
   `(let [q-fn# (-> (queries/query ~(str n) ~filename)
                    (assoc :ns *ns*)
                    (query-fn))]
      (create-root-var ~(str n) q-fn#)))
  ([filename]
   `(query-fns ~filename)))

(defn first! [coll]
  (or (first coll)
      (throw (ex-info "Record not found" {:type :404}))))

(defq "sql/schema.sql")

(defn create [db-name]
  (let [db-name (format "%s_%s" db-name (env/env :coast-env))
        sql (format "create database %s" db-name)]
    (exec (admin-connection) sql)
    (println "Database" db-name "created successfully")))

(defn drop [db-name]
  (let [db-name (format "%s_%s" db-name (env/env :coast-env))
        sql (format "drop database %s" db-name)]
    (exec (admin-connection) sql)
    (println "Database" db-name "dropped successfully")))

(defn defm [table]
  (create-root-var "insert" (fn [m]
                              (query (connection) (sql/insert table m))))
  (create-root-var "update" (fn update-fn
                              ([m where-clause]
                               (query (connection) (sql/update table m where-clause)))
                              ([m]
                               (first (query (connection) (sql/update table m))))))
  (create-root-var "delete" (fn [m]
                              (let [rows (query (connection) (sql/delete table m))]
                                (if (map? m)
                                  (first rows)
                                  rows))))
  (create-root-var "find-by" (fn [m]
                               (let [v (sql/v (sql/find-by table m) m)]
                                 (first (query (connection) v)))))
  (create-root-var "find" (fn [val]
                            (let [v (sql/v (sql/find table {:id val}) {:id val})]
                              (first! (query (connection) v)))))
  (create-root-var "query" (fn [& [m]]
                             (query (connection) (sql/v (sql/query table m)
                                                        (:where m)))))
  (create-root-var "find-or-create-by" (fn [m]
                                        (let [v (sql/v (sql/find-by table m) m)
                                              row (first (query (connection) v))]
                                          (if (nil? row)
                                            (first (query (connection) (sql/insert table m)))
                                            row))))
  nil)

(defn migrate-schema [schema]
  (jdbc/with-db-connection [conn (connection)]
    (jdbc/with-db-transaction [t conn]
      (let [create-statements (schema/create-tables-if-not-exists schema)
            col-statements (schema/add-columns schema)
            ident-statements (schema/add-idents schema)
            rel-statements (schema/add-rels schema)
            _ (doall
                (for [s create-statements]
                  (execute! t s)))
            _ (doall
                (for [s ident-statements]
                  (execute! t s)))
            _ (doall
                (for [s col-statements]
                  (execute! t s)))
            _ (doall
                (for [s rel-statements]
                  (execute! t s)))
            _ (schema/save schema)]
        (println "Schema applied successfully")))))

(defn pull [q ident]
  (coast.pull.db/pull (connection) (schema/fetch) q ident))

(defn fetch [ident]
  (coast.pull.db/fetch (connection) (schema/fetch) ident))

(defn insert [val]
  (coast.pull.db/insert (connection) (schema/fetch) val))

(defn update [m ident]
  (coast.pull.db/update (connection) (schema/fetch) m ident))

(defn delete [ident]
  (coast.pull.db/delete (connection) (schema/fetch) ident))
