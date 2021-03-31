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
  --output kafka|batch-grpc-collector
  --address grpc-collector.dev.batch.sh:9000 \
  --token your-token-here
```

# Performance

```bash
$ go run main.go --type search --count 100000 --batch-size 1000 --workers 20 --output batch-grpc-collector
4.31s user 0.93s system 86% cpu 6.078 total
```

# Available Types

* `all`
* `monitoring`
* `billing`
* `search`
* `audit`
* `reset_password`
