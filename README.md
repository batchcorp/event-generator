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
  --output batch-grpc-collector|kafka \
  --topic foo \
  --address grpc-collector.dev.batch.sh:9000 \
  --token your-token-here
```

# Continuous Mode

It is possible to run the event generator in "continuous" mode. This will keep
generating events and pushing them to the configured output.

It is possible to set a random send interval (based on a duration range such as
`1s:10s` or `1m:10m`) to simulate a more realistic event generation. You can also
set a random count (also using a range such as `1:100`).

Sending 1-1000 events every 1-10 seconds would look like this:

```bash
$ event-generator \
  --type search \
  --count 1:1000 \
  --token=9d793160-f0b8-4222-a7af-04806485a9da \
  --continuous \
  --continuous-interval 1s:10s \
  --sleep-random=100
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

* `all`
* `monitoring`
* `billing`
* `search`
* `audit`
* `reset_password`
