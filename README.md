# HighKue

[![Build Status](https://travis-ci.com/bfergerson/highkue.svg?branch=master)](https://travis-ci.com/bfergerson/highkue)

**HighKue** is a priority task queue developed with Vert.x and backed by **Redis**.
It's a Vert.x implementation version of [Automattic/kue](https://github.com/Automattic/kue) and fork of the original [sczyh30/vertx-kue](https://github.com/sczyh30/vertx-kue).

todo: doc differences and stuff

## Features

- Job priority
- Delayed jobs
- Process many jobs simultaneously
- Job and queue event
- Optional retries with backoff
- RESTful JSON API
- Rich integrated UI (with the help of Automattic/kue's UI)
- UI progress indication
- Job specific logging
- Future-based asynchronous model
- Polyglot language support
- Powered by Vert.x!

For the detail of the features, please see [HighKue Features](docs/en/vertx-kue-features-en.md).

## Build/Run

First build the code:

```
gradle build -x test
```

### Run in local

Vert.x Kue requires Redis running:

```
redis-server
```

Then we can run the example:

```
java -jar kue-core/build/libs/vertx-blueprint-kue-core.jar -cluster
java -jar kue-http/build/libs/vertx-blueprint-kue-http.jar -cluster
java -jar kue-example/build/libs/vertx-blueprint-kue-example.jar -cluster
```

Then you can visit `http://localhost:8080` to inspect the queue via Kue UI in the browser.

![](docs/images/vertx_kue_ui_1.png)

### Run with Docker Compose

To run HighKue with Docker Compose:

```
docker-compose up --build
```

Then you can run your applications in the terminal. For example:

```
java -jar kue-example/build/libs/vertx-blueprint-kue-example.jar -cluster
```

# Architecture

![Diagram - How Vert.x Kue works](https://raw.githubusercontent.com/sczyh30/vertx-kue/master/docs/images/kue_diagram.png)
