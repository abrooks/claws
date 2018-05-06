(ns net.brooks1.claws
  (:require [clojure.data.json :as json]
            [datascript.core :as d]
            [clojure.string :as str]
            [clojure.walk :as walk]
            #_[clojure.core.match :refer [match]]
            [clojure.spec :as s]))

;; http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-resource-specification.html
;; http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/cfn-resource-specification-format.html

(s/def ::spec
  (s/keys :req [::PropertyTypes ::ResourceTypes ::ResourceSpecificationVersion]))

(s/def ::ResourceSpecificationVersion string?)

(s/def ::PropertyTypes
  (s/map-of keyword? ::PropertyType :conform-keys true))

(s/def ::ResourceTypes
  (s/map-of keyword? ::ResourceType :conform-keys true))

(s/def ::PropertyType
  (s/keys :req [::Documentation ::Properties]))

(s/def ::ResourceType
  (s/keys :req [::Documentation ::Properties]
          :opt [::Attributes]))

(s/def ::Properties
  (s/map-of keyword? ::Property :conform-keys true))

(s/def ::Attributes
  (s/map-of keyword? ::Attribute :conform-keys true))

(s/def ::Property
  (s/or
   :Primitive    (s/merge ::PropertyBase (s/keys :req [::PrimitiveType]))
   :NonPrimitive (s/merge ::PropertyBase (s/keys :req [::Type]
                                                 :opt [::PrimitiveItemType ::ItemType]))))

(s/def ::PropertyBase
  (s/keys :req [::Documentation ::Required ::UpdateType]
          :opt [::DuplicatesAllowed]))

(s/def ::Attribute
  (s/keys :opt [::Type ::ItemType ::PrimitiveType ::PrimitiveItemType]))

(s/def ::Documentation string?)
(s/def ::DuplicatesAllowed boolean?)
(s/def ::Type string?)
(s/def ::PrimitiveType #{"String" "Long" "Integer" "Double" "Boolean" "Timestamp" "Json"})
(s/def ::ItemType string?)
;; The spec does not include "Json" among PrimitiveItemTypes
(s/def ::PrimitiveItemType #{"String" "Long" "Integer" "Double" "Boolean" "Timestamp" "Json"})
(s/def ::Required boolean?)
(s/def ::UpdateType #{"Mutable" "Immutable" "Conditional"})


;; +-Type
;; | +-PrimitiveType
;; | | +-ItemType
;; | | | +-PrimitiveItemType
;; | | | |
;; v v v v
;; . P . . primitive
;; T . . . nonprimitive
;; L . . P list primitive
;; M . . P map primitive
;; L . T . list nonprimitive
;; M . T . map nonprimitive

(def schema
  {"Type" {:db/valueType :db.type/ref}
   "PropertyName" {:db/unique :db.unique/identity}
   "ResourceName" {:db/unique :db.unique/identity}})

#_
(defn transact-prop-types! [spec conn]
  (do
    (d/transact! conn (for [[k v] (get spec "PropertyTypes")]
                        (assoc v "Name" k)))
    conn))

(defn ns-keywordize-keys
  "Recursively transforms all map keys from strings to keywords."
  {:added "1.1"}
  [n m]
  (let [f (fn [[k v]] (if (string? k) [(keyword n k) v] [k v]))]
    ;; only apply to maps
    (walk/postwalk (fn [x] (if (map? x) (into {} (map f x)) x)) m)))


;; PrimitiveType (primitive)
;; Type (map/list) -> PrimitiveItemType
;; Type (map/list) -> ItemType (composite)
;; Type (composite)

#_
(defn restruct-prop [ename pname prop]
  (let [m (assoc prop "PropertyName" pname)]
    (match [m]
           [{"PrimitiveItemType" _}])))


#_
(defn restruct-elem [ename etype elem]
  (-> elem
      (assoc etype ename)
      (assoc "Property" (mapv (partial restruct-prop ename) (get elem "Properties")))
      (dissoc "Properties")))



(comment

  (def spec-path "CloudFormationResourceSpecification.json")
  (def spec-txt (slurp spec-path))
  (def spec-json (json/read-str spec-txt))
  (def spec-edn (ns-keywordize-keys (namespace ::here) spec-json))
  (def conn (d/create-conn schema))

  (->>
   (get spec-json "PropertyTypes")
   (vals)
   (map #(get % "Properties"))
   (mapcat vals)
   (map #(get % "Type"))
   frequencies)


  (clojure.pprint/pprint
   (map keys
        (take 2
              (for [[k v] (get spec-json "PropertyTypes")]
                (assoc v "Name" k)))))

  (clojure.pprint/pprint
   (take 1
         (for [[k v] (get spec-json "ResourceTypes")]
           (assoc v "Name" k))))

  (def txn
    (d/transact! conn (for [[k v] (get spec-json "PropertyTypes")]
                        (-> v
                            (assoc "PropertyName" k)
                            (assoc "DataType" "Property")
                            (assoc "Property" (into []
                                                    (for [[pk pv] (get v "Properties")]
                                                      (let [m (assoc pv "PropertyName" pk)]
                                                        (if (get pv "Type")
                                                          m #_(update m "Type" #(identity {"PropertyName" %}))
                                                          m)))))
                            (dissoc "Properties")))))
  (def txn
    (d/transact! conn (for [[k v] (get spec-json "ResourceTypes")]
                        (-> v
                            (assoc "ResourceName" k)
                            (assoc "DataType" "Resource")
                            (assoc "Property" (into []
                                                    (for [[k v] (get v "Properties")]
                                                      (let [m (assoc v "PropertyName" k)]
                                                          (if (get v "PrimitiveType")
                                                            m
                                                            (let [t (get v "Type")]
                                                              (case t
                                                                "Map" (if-let [pt (get v "PrimitiveItemType")])
                                                                "List"
                                                                ()
                                                                m #_(update m "Type" #(identity {"PropertyName" %}))
                                                                m)))))))
                            (dissoc "Properties")))))

  (def prim-types (set (map first (d/q '[:find ?v :where [?e "PrimitiveType" ?v]] @conn))))
  (def colls #{"Map" "List"})

  (d/q '[:find ?a :where [?e ?a ?v]] @conn)
  (d/q '[:find ?e2 ?a ?v
         :where
         [?e "Name" "AWS::ElasticLoadBalancing::LoadBalancer.AccessLoggingPolicy"]
         [?e "Property" ?e2]
         [?e2 ?a ?v]] @conn)
 (d/q '[:find ?e
          :where
          [?e "Name" "AWS::ElasticLoadBalancing::LoadBalancer.AccessLoggingPolicy"]]
       @conn)
 (let [conn ((d/create-conn schema))])

 (q))
