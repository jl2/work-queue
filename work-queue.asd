;;;; work-queue.asd
;;;;
;;;; Copyright (c) 2017 Jeremiah LaRocco <jeremiah_larocco@fastmail.com>

(asdf:defsystem #:work-queue
  :description "Describe work-queue here"
  :author "Jeremiah LaRocco <jeremiah_larocco@fastmail.com>"
  :license "ISC (BSD-like)"
  :depends-on (#:bordeaux-threads)
  :serial t
  :components ((:file "package")
               (:file "work-queue")))

