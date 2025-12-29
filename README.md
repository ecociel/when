# when

A small Go package for scheduling asynchronous tasks.

Contains two executables:

- The [observer](./cmd/observer/) part that obtains due tasks from the task table and publishes
  them to the queue so that a worker can pick them up and run the corresponding action.
- A [demo](./cmd/demo/main.go) application that combines a scheduling part and a
  worker part. Normally, these will be distinct services in a larger system where one
  dedicated component (the worker) processes (if needed with >1 parallel instances)
  subscribes to the queue and executes tasks it receives.

# Build

    docker build -t when-demo --build-arg APP=demo .
    docker build -t when-observer --build-arg APP=observer .


# Run the Demo

   docker-compose up


# Visit the Red-Panda UI

   Point your browser to http://localhost:8000


