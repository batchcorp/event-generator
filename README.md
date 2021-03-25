event-generator
===============

Generate fake events and push them to grpc-collector in batches.

# Usage

```bash
event-generator --type search \
  --count 100000 \
  --batch-size 1000 \
  --grpc-collector-address grpc-collector.dev.batch.sh:9000 \
  --token your-token-here
```

# Performance

```bash
$ go run main.go --type search --count 100000 --batch-size 1000 --workers 20
4.31s user 0.93s system 86% cpu 6.078 total
```

# Available Types

* `all`
* `monitoring`
* `billing`
* `search`
* `audit`
* `reset_password`
