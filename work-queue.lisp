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
   (job-cv :initform nil :initarg :job-cv)))

(defun worker-thread (wq)
  (bt:make-thread
   (lambda ()
     (let ((current-job nil)
           (work-done nil))
       (loop until work-done
          do
            (bt:condition-wait (slot-value wq 'job-cv) (slot-value wq 'job-mutex))
            (unwind-protect
                 (with-slots (jobs finished) wq
                   (when jobs
                     (setf current-job (pop jobs)))
                   (when (and (null current-job) (null jobs) finished)
                     (setf work-done t)))
              (bt:release-lock (slot-value wq 'job-mutex)))
            (when current-job
              (with-slots (consumer-function) wq
                (funcall consumer-function current-job))
              (setf current-job nil)))))))
           


(defun create-work-queue (consumer &optional (thread-count 8))
  (let ((wq (make-instance 'work-queue
                           :thread-count thread-count
                           :job-mutex (bt:make-lock "job-queue-lock")
                           :job-cv (bt:make-condition-variable :name "job-cv")
                           :consumer consumer)))
    (with-slots (threads jobs) wq
      (dotimes (i thread-count)
        (push (worker-thread wq) threads)))
    wq))

(defun add-job (wq item)
  (when (slot-value wq 'finished)
    (error "Work queue is already finished."))
  (bt:acquire-lock (slot-value wq 'job-mutex))
  (unwind-protect 
       (with-slots (job-cv jobs) wq
         (push item jobs))
    (bt:release-lock (slot-value wq 'job-mutex)))
  (bt:condition-notify (slot-value wq 'job-cv))
  wq)

(defun destroy-work-queue (wq)
  (bt:acquire-lock (slot-value wq 'job-mutex))
  (unwind-protect 
       (with-slots (job-cv finished) wq
         (setf finished t)
         (bt:condition-notify job-cv))
    (bt:release-lock (slot-value wq 'job-mutex)))
  (dolist (thread (slot-value wq 'threads))
    (bt:join-thread thread))
  wq)

(defun stop-work-queue (wq)
  wq)
        
