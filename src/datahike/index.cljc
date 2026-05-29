(ns ^:no-doc datahike.index
  (:refer-clojure :exclude [-persistent! -flush -count -seq])
  (:require [datahike.index.interface :as di]
            [datahike.index.persistent-set]))

;; Wrappers for protocol functions. Keep these as forwarding functions rather
;; than value aliases so protocol extensions loaded after this namespace (for
;; example the optional hitchhiker-tree index) are visible to callers here.

(defn -all [index]
  (di/-all index))

(defn -seq [index]
  (di/-seq index))

(defn -count [index]
  (di/-count index))

(defn -insert [index datom index-type op-count]
  (di/-insert index datom index-type op-count))

(defn -temporal-insert [index datom index-type op-count]
  (di/-temporal-insert index datom index-type op-count))

(defn -upsert [index datom index-type op-count old-datom]
  (di/-upsert index datom index-type op-count old-datom))

(defn -temporal-upsert [index datom index-type op-count old-datom]
  (di/-temporal-upsert index datom index-type op-count old-datom))

(defn -remove [index datom index-type op-count]
  (di/-remove index datom index-type op-count))

(defn -slice [index from to index-type]
  (di/-slice index from to index-type))

(defn -lookup [index key cmp]
  (di/-lookup index key cmp))

(defn -count-slice [index from to cmp]
  (di/-count-slice index from to cmp))

(defn -has-subtree-counts? [index]
  (di/-has-subtree-counts? index))

(defn -flush [index backend]
  (di/-flush index backend))

(defn -transient [index]
  (di/-transient index))

(defn -persistent! [index]
  (di/-persistent! index))

(defn -mark [index]
  (di/-mark index))

;; Aliases for multimethods

(def empty-index di/empty-index)
(def init-index di/init-index)
(def add-konserve-handlers di/add-konserve-handlers)
(def konserve-backend di/konserve-backend)
(def default-index-config di/default-index-config)

;; Other functions

(def index-types (keys (methods empty-index)))
