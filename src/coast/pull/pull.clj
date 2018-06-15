(ns coast.pull.pull
  (:require [clojure.java.jdbc :as jdbc]
            [clojure.string :as string]
            [coast.utils :as utils]
            [clojure.set]))

(defn flatten-keys* [a ks m]
  (if (map? m)
    (reduce into (map (fn [[k v]] (flatten-keys* a (conj ks k) v)) (seq m)))
    (assoc a ks m)))

(defn flatten-keys [m] (flatten-keys* {} [] m))

(defn val->map [val]
  (cond
    (keyword? val) {val nil}
    (map? val) (->> (map (fn [[k v]] [k (val->map v)]) val)
                    (into {}))
    (vector? val) (->> (map val->map val)
                       (apply merge))
    :default {}))

(defn paths [v]
  (->> (apply merge (map val->map v))
       (flatten-keys)
       (keys)))

(defn rels [schema v]
  (let [rel-keys (->> (paths v)
                      (mapcat identity)
                      (set)
                      (clojure.set/intersection (:rels schema)))]
    (select-keys schema rel-keys)))

(defn join [k]
  (let [namespace (namespace k)
        name (name k)
        join-left (str namespace "." name "_id")
        join-right (str name ".id")]
    (str "left outer join " namespace " on " join-left " = " join-right)))

(defn column [k]
  (when (qualified-keyword? k)
    (str (namespace k) "." (utils/snake (name k)))))

(defn nested-col [ks]
  (str (column (last ks)) " as " (->> (map #(str (namespace %) "_" (name %)) ks)
                                      (string/join "__"))))

(defn nested? [ks]
  (> (count ks) 1))

(defn add-idx [ks]
  (if (nested? ks)
    (let [indexes (->> (map namespace ks)
                       (rest)
                       (map #(keyword % "idx$"))
                       (vec))]
      (vec (drop-last (interleave ks (conj indexes nil)))))
    ks))

(defn partition-by-part [coll]
  (let [cols (->> (drop-last coll)
                  (map #(str % ".id"))
                  (string/join ","))]
    (if (string/blank? cols)
      ""
      (format "partition by %s" cols))))

(defn row-number [coll]
  (when (and (coll? coll)
             (not (empty? coll)))
    (let [partition-by-str (partition-by-part coll)]
      (format "row_number() over(%s order by %s.id) - 1 as %s_idx$"
              partition-by-str
              (last coll)
              (last coll)))))

(defn dense-rank [coll]
  (if (> (count coll) 1)
    (map
      #(format "dense_rank() over(order by %s.id) - 1 as %s_idx$" % %)
      (drop-last coll))
    []))

(defn rank-cols [paths]
  (let [rel-cols (->> (map #(map namespace %) paths)
                      (map #(drop 1 %))
                      (filter #(not (empty? %)))
                      (distinct))
        max-rel-col (if (empty? rel-cols)
                      nil
                      (apply max-key count rel-cols))
        row-number (row-number max-rel-col)
        dense-ranks (dense-rank max-rel-col)]
    (when (and (some? row-number)
               (every? some? dense-ranks))
      (concat [row-number] dense-ranks))))

(defn select [v]
  (let [ps (paths v)
        cols (->> ps
                  (map add-idx)
                  (map nested-col))
        rank-cols (rank-cols ps)]
    (->> (string/join ", " (concat cols rank-cols))
         (str "select "))))

(defn order [v]
  (->> (paths v)
       (mapcat identity)
       (map namespace)
       (distinct)
       (map #(str % ".id asc"))
       (string/join ", ")))

(defn sql-vec [schema v ident]
  (let [select (select v)
        from (str "from " (-> ident first namespace))
        joins (->> (rels schema v)
                   (vals)
                   (map :db/joins)
                   (map join)
                   (string/join " "))
        where (str "where " (-> ident first column) " = ?")
        order (str "order by " (order v))
        where-clause [(second ident)]
        sql (string/join " " [select from joins where order])]
    (apply conj [sql] where-clause)))

(defn sql-vec? [v]
  (and (vector? v)
       (string? (first v))
       (not (string/blank? (first v)))))

(defn query [conn v]
  (if (sql-vec? v)
    (jdbc/query conn v {:keywordize? false})
    '()))

(defn flatten* [val]
  (if (coll? val)
    (flatten val)
    val))

(defn distinct* [val]
  (if (coll? val)
    (distinct val)
    val))

(defn deepest-path [rows]
  (->> (map #(->> % keys (apply max-key count)) rows)
       (first)))

(defn qualify-keyword [k]
  (if (keyword? k)
    (let [[n name] (-> (name k)
                       (string/split #"_"))]
      (keyword n name))
    k))

(defn nest [m]
  (reduce (fn [acc [k v]]
            (let [key-strs (-> k (name) (string/split #"[_]{2}"))]
              (assoc-in acc (mapv #(-> % keyword qualify-keyword) key-strs) v)))
          {}
          m))


(defn map-vals [f m]
 (->> (map (fn [[k v]] [k (f v)]) m)
      (into {})))

(defn merge-lists [maps]
  (reduce (fn [m1 m2]
            (reduce (fn [m [k v]]
                      (update-in m [k] (fnil conj []) v))
                    m1, m2))
          {}
          maps))

(defn rel-paths [nested-row rels]
  (->> nested-row flatten-keys keys
       (map (fn [ks]
              (filter (fn [k] (contains? rels k)) ks)))
       (filter #(not (empty? %)))
       (distinct)))

(defn qualify-col [s]
  (let [[n name] (string/split s #"_")]
    (keyword n name)))
