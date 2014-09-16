(ns bala.core
  (:require [server.socket :as ss]
            [clj-yaml.core :as yaml])
  (:gen-class))

(def buffer-size 8192)
(def ^:dynamic props nil)

(defn connector-fn
  [fn-name]
  (resolve (symbol (str (-> props :connector) fn-name))))

(defn prox
  [server qbuf]
  (let [expect-response? (if-let [exp-fn (connector-fn "/expect-response?")]
                           (exp-fn qbuf)
                           true)
        sbuf
        (try
	  (let [sock (java.net.Socket. (-> server :host) (-> server :port))
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
	      
	  (catch java.net.SocketTimeoutException e
	    (println (format "%s:%d: %s"
			     (-> server :host)
			     (-> server :port)
			     (.getMessage e)))
	    (java.io.ByteArrayOutputStream. 0))
	  (catch java.net.ConnectException e
	    (println (format "%s:%d: %s"
			     (-> server :host)
			     (-> server :port)
			     (.getMessage e)))
	      (java.io.ByteArrayOutputStream. 0)))]
      {:buffer sbuf :server server}))

  (defn intercept
    [qbuf]
    (let [responses (pmap
		      #(prox % qbuf)
		      (-> props :servers))
	  primary (first
		    (filter
		      #(= (:primary (second %)) true)
		      (map-indexed vector (-> props :servers))))]

      (let [pkg {:responses responses
                 :request qbuf
                 :props props}]
        ((connector-fn "/handle-interchange") pkg))
      
      (if primary
        (:buffer (nth responses (first primary)))
        (:buffer (first responses)))))

  (defn handle-connection
    [conn]
    (loop []
      (let [ba ((connector-fn "/read-request") (:in @conn))]
	(if (not= -1 ba)
        (let [qbuf (java.io.ByteArrayOutputStream. (count ba))]
          (-> qbuf (.write ba 0 (count ba)))
	  (.writeTo (-> qbuf intercept) (:out @conn))
          (recur))
        (println "connection closed")))))

(defn handle-thread
  [in out]
  (when-let [pre-fn (connector-fn "/before-connection")]
    (pre-fn props))
  (let [conn (ref {:in in :out out})]
    (handle-connection conn))
  (when-let [post-fn (connector-fn "/after-connection")]
    (post-fn props)))

(defn -main
  [& [properties-file]]
  (if properties-file
    (do
      (alter-var-root #'props
                      (fn [_] (yaml/parse-string (slurp properties-file))))
      (require (symbol (-> props :connector)))
      (when-let [pre-server-fn (connector-fn "/before-create-server")]
        (pre-server-fn props))
      (ss/create-server (-> props :proxy-port) handle-thread))
    (println "usage: [main] /path/to/config/file")))

