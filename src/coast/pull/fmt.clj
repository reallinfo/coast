(ns coast.pull.fmt
  (:require [clojure.string :as string]
            [clojure.walk :as walk]))

(defn map-keys [f m]
  (->> (map (fn [[k v]] [(f k) v]) m)
       (into (empty m))))

(defn single [coll]
  (if (and (= 1 (count coll))
           (coll? coll))
    (first coll)
    coll))

(defn path [s]
  (single (string/split s #"__")))

(defn path-key [m v]
  (if (vector? v)
    (mapv #(get m % %) v)
    v))

(defn path-row [m]
  (->> (map (fn [[k v]] [(path-key m k) v]) m)
       (into (empty m))))

(defn cols [rows]
  (keys (first rows)))

(defn vectorize [val]
  (if (vector? val)
    val
    [val]))

(defn dissoc-path-keys [m]
  (let [ks (->> (keys m)
                (filter #(string/ends-with? % "_idx$")))]
    (apply dissoc m ks)))

(defn nest [m]
  (reduce (fn [acc [k v]]
            (assoc-in acc k v))
          {}
          m))

(defn map->vec [m]
  (if (and (map? m)
           (or (every? integer? (keys m))
               (every? empty? (vals m))))
    (or (->> m vals
             (filter some?)
             (single)
             (vec))
        [])
    m))

(defn qualify-keyword [s]
  (if (string? s)
    (let [[namespace name] (string/split s #"_")]
      (keyword namespace name))
    s))

(defn qualify-map [val]
  (if (and (map? val)
           (every? string? (keys val)))
    (map-keys qualify-keyword val)
    val))

(defn fmt-pull-rows [rows]
  (let [merged-map (->> (map #(map-keys path %) rows)
                        (map path-row)
                        (map dissoc-path-keys)
                        (map #(map-keys vectorize %))
                        (apply merge)
                        (nest))]
    (->> (walk/postwalk map->vec merged-map)
         (walk/postwalk qualify-map))))
