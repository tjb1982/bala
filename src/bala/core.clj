(ns bala.core
  (:require [server.socket :as ss]
            [clj-yaml.core :as yaml])
  (:gen-class))

(def buffer-size 8192)
(def ^:dynamic props nil)

(defn prox
  [server qbuf]
  (try
    (let [sock (java.net.Socket. (-> server :name) (-> server :port))
          in (-> sock .getInputStream)
          out (-> sock .getOutputStream)]
      (.setSoTimeout sock (or (-> server :timeout) (-> props :server-timeout) 0))
      (.writeTo qbuf out)
      (let [first-byte (.read in)]
        (if (not= -1 first-byte)
          (let [leftover (.available in)
                ba (byte-array leftover)
                sbuf (java.io.ByteArrayOutputStream. (inc leftover))]
            (-> sbuf (.write first-byte))
            (-> in (.read ba 0 (.available in)))
            (-> sbuf (.write ba 0 leftover))
	    sbuf)
	  (java.io.ByteArrayOutputStream. 0))))
    (catch java.net.SocketTimeoutException e
      (println (format "%s:%d: %s" (-> server :name) (-> server :port) (.getMessage e)))
      (java.io.ByteArrayOutputStream. 0))
    (catch java.net.ConnectException e
      (println (format "%s:%d: %s" (-> server :name) (-> server :port) (.getMessage e)))
      (java.io.ByteArrayOutputStream. 0))))

(defn intercept
  [qbuf]
  (let [responses (pmap
                    #(prox % qbuf)
                    (-> props :servers))
        primary (first
                  (filter
                    #(= (:primary (second %)) true)
                    (map-indexed vector (-> props :servers))))]

    (if primary
      (nth responses (first primary))
      (first responses))))

(defn handle-connection
  [conn]
  (loop []
    (let [first-byte (-> (:in @conn) .read)]
      (if (not= -1 first-byte)
        (let [leftover (.available (:in @conn))
              ba (byte-array leftover)
              qbuf (java.io.ByteArrayOutputStream. (inc leftover))]

          (-> qbuf (.write first-byte))
          (-> (:in @conn) (.read ba 0 (.available (:in @conn))))
          (-> qbuf (.write ba 0 leftover))

          (let [sbuf (intercept qbuf)]
	    (.writeTo sbuf (:out @conn)))

          (-> qbuf .reset)
          (recur))
        (println "connection closed")))))

(defn handle-thread
  [in out]
  (let [conn (ref {:in in :out out})]
    (handle-connection conn)))

(defn -main
  [& [properties-file _]]
  (if properties-file
    (do
      (alter-var-root #'props
                      (fn [_] (yaml/parse-string (slurp properties-file))))
      (ss/create-server (-> props :proxy-port) handle-thread))
    (println "usage: [main] /path/to/config/file")))

