(ns coast.cli
  (:require [coast.db :as db]
            [coast.utils :as utils]
            [coast.migrations :as migrations]))

(defn -main [& args]
  (let [[action db-name] args
        db-name (utils/snake db-name)]
    (case action
      "db:create" (db/create db-name)
      "db:drop" (db/drop db-name)
      "db:migrate" (migrations/migrate)
      "db:rollback" (migrations/rollback)
      "")))

(comment
  (def schema [{:db/ident :member/email
                :db/type "text"}

               {:db/ident :member/name
                :db/type "text"}

               {:db/rel :member/projects
                :db/type :many
                :db/joins :project/member}

               {:db/col :project/name
                :db/type "text"
                :db/nil? false}

               {:db/rel :member/todos
                :db/type :many
                :db/joins :todo/member}

               {:db/col :todo/name
                :db/type "text"
                :db/nil? false}

               {:db/rel :todo/tags
                :db/type :many
                :db/joins :tag/todo}

               {:db/col :tag/name
                :db/type "text"
                :db/nil? false}])

  (db/migrate-schema schema)
  (coast.schema/save schema)

  (db/insert {:member/email "swlkr@fastmail.com"
              :member/name "swlkr"})

  (db/insert [{:project/name "project #1"
               :project/member [:member/name "swlkr"]}
              {:project/name "project #2"
               :project/member [:member/name "swlkr"]}
              {:project/name "project #3"
               :project/member [:member/name "swlkr"]}])

  (db/insert [{:todo/name "todo 1"
               :todo/slug "todo-1"
               :todo/member [:member/name "swlkr"]}
              {:todo/name "todo 2"
               :todo/slug "todo-2"
               :todo/member [:member/name "swlkr"]}])

  (db/insert [{:tag/name "tag 1"
               :tag/todo-id 1}
              {:tag/name "tag 2"
               :tag/todo-id 1}])

  (db/insert [{:todo/name "todo 3"
               :todo/slug "todo-3"
               :todo/member [:member/name "swlkr"]}
              {:todo/name "todo 4"
               :todo/slug "todo-4"
               :todo/member [:member/name "swlkr"]}])

  (db/insert [{:tag/name "tag 3"
               :tag/todo [:todo/slug "todo-2"]}
              {:tag/name "tag 4"
               :tag/todo [:todo/slug "todo-2"]}])

  (db/pull [:member/name
            {:member/projects [:project/name]}
            {:member/todos [:todo/name
                            {:todo/tags [:tag/name]}]}]
           [:member/name "swlkr"])

  (db/pull [:member/name
            :member/email
            :member/id
            {:member/todos [:todo/name
                            :todo/slug
                            {:todo/tags [:tag/name]}]}]
           [:member/name "swlkr"]))
