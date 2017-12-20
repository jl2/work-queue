;;;; package.lisp
;;;;
;;;; Copyright (c) 2017 Jeremiah LaRocco <jeremiah_larocco@fastmail.com>

(defpackage #:work-queue
  (:nicknames #:wq)
  (:use #:cl)
  (:export #:worker-thread
           #:create-work-queue
           #:add-job
           #:destroy-work-queue
           #:worker-queue))

