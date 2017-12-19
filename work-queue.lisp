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
   (finish-cv :initform nil :initarg :finish-cv)))

(defun worker-thread (wq)
  (bt:make-thread
   (lambda ()

     (with-slots (jobs finished finish-mutex finish-cv job-mutex job-cv consumer-function) wq
       (let ((current-job nil)
             (work-done nil))

         (loop until work-done
            while (bt:with-lock-held (job-mutex)
                    (format t "waiting on jobs.~%")
                    jobs)
            do
              (cond ((bt:with-lock-held (job-mutex) (format t "waiting on jobs.~%") jobs)
                     (bt:with-lock-held (job-mutex)
                       (format t "waiting on jobs.~%")
                       (when jobs
                         (setf current-job (pop jobs)))))
                    (current-job
                     (funcall consumer-function current-job))
                    ((bt:with-lock-held (job-mutex)
                       (format t "waiting on finished.~%")finished)
                     (setf work-done t))
                    (t
                     (bt:condition-wait job-cv job-mutex)))))))))


(defun create-work-queue (consumer &optional (thread-count 8))
  (let ((wq (make-instance 'work-queue
                           :thread-count thread-count
                           :job-mutex (bt:make-lock "job-queue-lock")
                           :job-cv (bt:make-condition-variable :name "job-cv")
                           :finish-mutex (bt:make-lock "finish-queue-lock")
                           :finish-cv (bt:make-condition-variable :name "finish-cv")
                           :consumer consumer)))
    (with-slots (threads jobs) wq
      (dotimes (i thread-count)
        (push (worker-thread wq) threads)))
    wq))

(defun add-job (wq item)
  (with-slots (finished job-mutex job-cv jobs) wq
    
    (bt:with-lock-held (job-mutex)
      (when finished
        (error "Work queue is already finished."))
      (bt:condition-notify (slot-value wq 'job-cv)))))

(defun destroy-work-queue (wq)
  (with-slots (job-cv job-mutex finished finished-mutex finished-cv threads) wq
    (dotimes (i (length threads))
      (bt:with-lock-held (job-mutex)
        (setf finished t)
        (bt:condition-notify job-cv))
      (bt:with-lock-held (finished-mutex)
        (bt:condition-wait finished-mutex finished-cv)))
    (dolist (thread threads)
      (bt:join-thread thread))
    wq))

(defun stop-work-queue (wq)
  wq)
        
