(ns bala.core
  (:require [server.socket :as ss]
            [clojure.pprint :as pprint]
            [clj-yaml.core :as yaml])
  (:gen-class))

(def buffer-size 8192)
(def ^:dynamic props nil)
(def c (atom 0))
(def ^:dynamic servers nil)

(defn connector-fn
  [fn-name]
  (resolve (symbol (str (-> props :connector) fn-name))))

(defn print-server-exception
  [server e]
  (println (format "%s:%d: %s" (:host server) (:port server) (.getMessage e))))

(defn prox
  [server qbuf]
  (let [expect-response? (if-let [exp-fn (connector-fn "/expect-response?")]
                           (exp-fn qbuf)
                           true)
        sbuf
        (try
	  (let [sock (if (:isolate-requests server)
                           (java.net.Socket. (-> server :host) (-> server :port))
                           (:sock server))
		in (-> sock .getInputStream)
		out (-> sock .getOutputStream)]
	    (.setSoTimeout sock (or (-> server :timeout) (-> props :server-timeout) 0))
	    (.writeTo qbuf out)
     
	    (if expect-response?
	      (let [ba ((connector-fn "/read-response") in)]
		(if (not= -1 ba)
		  (let [sbuf (java.io.ByteArrayOutputStream. (count ba))]
		    (-> sbuf (.write ba 0 (count ba)))
		    sbuf)
		  (java.io.ByteArrayOutputStream. 0)))
              (java.io.ByteArrayOutputStream. 0)))
	  (catch java.net.NoRouteToHostException e
            (print-server-exception server e)
	    (java.io.ByteArrayOutputStream. 0))
	  (catch java.net.SocketTimeoutException e
            (print-server-exception server e)
	    (java.io.ByteArrayOutputStream. 0))
	  (catch java.net.ConnectException e
            (print-server-exception server e)
	    (java.io.ByteArrayOutputStream. 0)))]
      {:buffer sbuf :server server}))

(defn intercept
  [qbuf]
  (let [responses (let [m (if (= (props :sync) true) map pmap)]
                    (m
                      #(prox % qbuf)
                      servers))
        primary (first
      	    (filter
      	      #(= (:primary (second %)) true)
      	      (map-indexed vector servers)))]

    (let [pkg {:responses responses
               :request qbuf
               :props props}]
      ((connector-fn "/handle-interchange") pkg))
    
    (if primary
      (:buffer (nth responses (first primary)))
      (:buffer (first responses)))))


(defn handle-connection
  [conn]
  (println "new connection:" (swap! c inc))
  (loop []
    (let [ba ((connector-fn "/read-request") (:in @conn))]
      (if (not= -1 ba)
      (let [qbuf (java.io.ByteArrayOutputStream. (count ba))]
        (-> qbuf (.write ba 0 (count ba)))
        (.writeTo (-> qbuf intercept) (:out @conn))
        (recur))
      (println "connection closed:" @(:id @conn))))))

(defn handle-thread
  [in out]
  (when-let [pre-fn (connector-fn "/before-connection")]
    (pre-fn props))
  (let [conn (ref {:in in :out out :id c})]
    (handle-connection conn))
  (when-let [post-fn (connector-fn "/after-connection")]
    (post-fn props)))

(defn -main
  [& [properties-file]]
  (if properties-file
    (do
      (alter-var-root #'props
                      (fn [_] (yaml/parse-string (slurp properties-file))))
      (alter-var-root #'servers
                      (fn [_] (map
                                (fn [server]
                                  (let [sock (java.net.Socket. (:host server) (:port server))]
                                    (assoc server 
                                      :sock sock)))
                                (:servers props))))
      (println "Configured for:") (pprint/pprint servers)
      (require (symbol (:connector props)))
      (println "Using connector module:" (:connector props))
      (when-let [pre-server-fn (connector-fn "/before-create-server")]
        (pre-server-fn props))
      (ss/create-server (-> props :proxy-port) handle-thread 20))
    (println "usage: [main] /path/to/config/file")))

