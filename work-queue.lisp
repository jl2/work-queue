;;;; work-queue.lisp
;;;;
;;;; Copyright (c) 2017 Jeremiah LaRocco <jeremiah_larocco@fastmail.com>

(in-package #:work-queue)

(defclass work-queue ()
  ((thread-count :initform 8 :initarg :thread-count)
   (consumer-function :initarg :consumer)
   (finished :initform nil)
   (threads :initform nil)
   (jobs :initform nil :initarg :jobs)
   (job-mutex :initform nil :initarg :job-mutex)
   (job-cv :initform nil :initarg :job-cv)
   (finish-mutex :initform nil :initarg :finish-mutex)
   (finish-cv :initform nil :initarg :finish-cv))
  (:documentation "A multi-threaded job queue."))

(defun create-worker-thread (wq)
  "Create a worker thread that waits for jobs and calls consumer-function with them."
  (bt:make-thread
   (lambda ()
     (with-slots (jobs finished finish-mutex finish-cv job-mutex job-cv consumer-function) wq

       (let ((current-job nil)
             (work-done nil))

         (loop until work-done
            do
              (when current-job
                (funcall consumer-function current-job)
                (setf current-job nil))

              (bt:with-lock-held (job-mutex)
                (cond (jobs
                       (setf current-job (pop jobs))
                       (setf work-done nil))
                      (finished
                       (setf work-done t))
                      (t
                       (bt:condition-wait job-cv job-mutex))))))
       (format t "Thread notifiying finish-cv.~%")
       (bt:with-lock-held (finish-mutex)
         (bt:condition-notify finish-cv)))
       (format t "quitting ~%"))))


(defun create-work-queue (consumer &optional (thread-count 8))
  "Create a work queue object and launch the specified number of threads."
  (let ((wq (make-instance 'work-queue
                           :thread-count thread-count
                           :job-mutex (bt:make-lock)
                           :job-cv (bt:make-condition-variable)
                           :finish-mutex (bt:make-lock)
                           :finish-cv (bt:make-condition-variable)
                           :consumer consumer)))
    (with-slots (threads jobs) wq
      (dotimes (i thread-count)
        (push (create-worker-thread wq) threads)))
    wq))

(defun add-job (wq item)
  "Add a job to the work queue."
  (with-slots (finished job-mutex job-cv jobs) wq
    (bt:with-lock-held (job-mutex)
      (when finished
        (error "Work queue is already finished."))
      (push item jobs))
    (bt:condition-notify job-cv)))

(defun destroy-work-queue (wq)
  "Wait for all jobs to finish and join all the threads."
  (with-slots (job-cv job-mutex finished finish-mutex finish-cv threads) wq
    (bt:with-lock-held (finish-mutex)
      (setf finished t))
    (dotimes (i (length threads))
      (bt:condition-notify job-cv))
    (dolist (thread threads)
      (bt:join-thread thread))
    wq))

