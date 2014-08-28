(defproject bala "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [
		[org.clojure/clojure "1.6.0"]
		[server-socket/server-socket "1.0.0"]
                [clj-yaml "0.4.0"]
		]
  :main ^:skip-aot bala.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
