(ns bala.core
  (:require [server.socket :as ss]
            [clj-yaml.core :as yaml])
  (:gen-class))

(def buffer-size 8192)
(def ^:dynamic props nil)

(defn prox
  [server qbytes qlen]
  (let [sbuf (java.io.ByteArrayOutputStream. buffer-size)]
    (try
      (let [sock (java.net.Socket. (-> server :name) (-> server :port))
	    in (java.io.InputStreamReader. (-> sock .getInputStream))
	    out (-> sock .getOutputStream)]
	(-> out (.write qbytes 0 qlen))
	(let [first-byte (.read in)]
	  (when (not= -1 first-byte)
	    (do
	      (-> sbuf (.write first-byte))
	      (while (.ready in)
		(.write sbuf (.read in)))))
	  sbuf))
      (catch java.net.ConnectException e sbuf))))

(defn intercept
  [qbuf]
  (let [bufsize (.size qbuf)
        q (.toByteArray qbuf)
        responses (pmap
                    #(prox % q bufsize)
                    (-> props :servers))
        primary-response (first responses)]

    (println (apply str (map #(format "%02x:" %) q)))
    (println (map #(char (bit-and % 255)) q))

    (println (first responses) (str (first responses)))

    primary-response))

(defn handle-connection
  [conn]
  (let [qbuf (java.io.ByteArrayOutputStream. buffer-size)]
    (loop []
      (let [first-byte (-> (:in @conn) .read)]
	(if (not= -1 first-byte)
	  (do
	    (-> qbuf (.write first-byte))
	    (while (.ready (:in @conn))
	      (-> qbuf (.write (-> (:in @conn) .read))))

	    (let [sbuf (intercept qbuf)
                  slen (.size sbuf)
		  s (.toByteArray sbuf)]
	      (-> (:out @conn) (.write s 0 slen)))

	    (-> qbuf .reset)
	    (recur))
	  (println "connection closed"))))))

(defn handle-thread
  [in out]
  (let [in (java.io.InputStreamReader. in)
        conn (ref {:in in :out out})]
    (handle-connection conn)))

(defn -main
  [properties-file & _]
  (alter-var-root #'props
                  (fn [_] (yaml/parse-string (slurp properties-file))))
  (ss/create-server (-> props :proxy-server :port) handle-thread))
