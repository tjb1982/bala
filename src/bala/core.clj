(ns bala.core
  (:require [server.socket :as ss]
            [clj-yaml.core :as yaml])
  (:gen-class))

(def buffer-size 8192)
(def ^:dynamic props nil)

(defn prox
  [server qbuf]
  (let [sbuf
    (try
      (let [sock (java.net.Socket. (-> server :host) (-> server :port))
	    in (-> sock .getInputStream)
	    out (-> sock .getOutputStream)]
	(.setSoTimeout sock (or (-> server :timeout) (-> props :server-timeout) 0))
	(.writeTo qbuf out)
          
        (let [ba (-> in ((resolve (symbol (str (-> props :connector) "/read-response")))))]
          (if (not= -1 ba)
            (let [sbuf (java.io.ByteArrayOutputStream. (count ba))]
              (-> sbuf (.write ba 0 (count ba)))
	      sbuf)
	    (java.io.ByteArrayOutputStream. 0))))
          
      (catch java.net.SocketTimeoutException e
	(println (format "%s:%d: %s" (-> server :host) (-> server :port) (.getMessage e)))
	(java.io.ByteArrayOutputStream. 0))
      (catch java.net.ConnectException e
	(println (format "%s:%d: %s" (-> server :host) (-> server :port) (.getMessage e)))
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

    (let [connector (-> props :connector)]
      (let [pkg {:responses responses
                 :request qbuf
                 :props props}]
        ((resolve (symbol (str connector "/handle-interchange"))) pkg)))
    
    (if primary
      (:buffer (nth responses (first primary)))
      (:buffer (first responses)))))

(defn handle-connection
  [conn]
  (loop []
    (let [ba ((resolve (symbol (str (-> props :connector) "/read-request")))
               (:in @conn))]
      (if (not= -1 ba)
        (let [qbuf (java.io.ByteArrayOutputStream. (count ba))]
          (-> qbuf (.write ba 0 (count ba)))
          (let [sbuf (-> qbuf intercept)]
            (.writeTo sbuf (:out @conn)))
          (recur))
        (println "connection closed")))))

(defn handle-thread
  [in out]
  (let [conn (ref {:in in :out out})]
    (handle-connection conn)))

(defn -main
  [& [properties-file]]
  (if properties-file
    (do
      (alter-var-root #'props
                      (fn [_] (yaml/parse-string (slurp properties-file))))
      (require (symbol (-> props :connector)))
      (ss/create-server (-> props :proxy-port) handle-thread))
    (println "usage: [main] /path/to/config/file")))

