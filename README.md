event-generator
===============

Generate fake events and push them to various outputs such as Kafka or directly
to the Batch gRPC collectors.

# Usage

```bash
event-generator \
  --type search \
  --count 100000 \
  --batch-size 1000 \
  --output batch-grpc-collector \
  --address grpc-collector.dev.batch.sh:9000 \
  --token $COLLECTION_TOKEN
```

# Continuous Mode

It is possible to run the event generator in "continuous" mode. This will keep
generating events and pushing them to the configured output.

You can set a random send interval (based on a duration range such as `1s:10s` 
or `1m:10m`) to simulate a more realistic event generation. You can also set a 
random count (also using a range such as `1:100`).

Sending 1-1000 events every 1-10 seconds, with a 1s to 5s random sleep (in-between sends) would look like this:

```bash
$ event-generator \
  --type search \
  --count 1:1000 \
  --token=9d793160-f0b8-4222-a7af-04806485a9da \
  --continuous 1s:10s \
  --continuous-interval 1s:10s \
  --sleep 1s:5s
```

# Performance

```bash
$ time go run main.go \
  --type search 
  --count 100000 \
  --batch-size 1000 \
  --workers 20 \
  --destination-type batch-grpc-collector

4.31s user 0.93s system 86% cpu 6.078 total
```

# Available Types

* `billing`
* `search`
* `products`
* `users`
* `posts`
* `weather`
* `coins`

NOTE: API shape for the fake events is inspired by real, public APIs, dummyjson.com
and _imagination_. Needless to say, the data is not real :-)

# Schema

All events use the schema defined in the [batchcorp/schemas](https://github.com/batchcorp/schemas/tree/master/fakes/event-generator) repo. 

All generated events use the `fakes.Event` envelope.
