package main

import (
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/batchcorp/schemas/build/go/events/fakes"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/event-generator/cli"
)

func sendNoOpEvents(wg *sync.WaitGroup, params *cli.Params, id string, generateChan chan *fakes.Event) {
	defer wg.Done()

	id = "noop-" + id

	logrus.Infof("worker id '%s' started", id)

	batch := make([][]byte, 0)

	batchSize := params.BatchSize
	numEvents := 0
	iter := 0
	numFudgedEvents := 0
	fudgeEvery := 0

	// Figure out how often to fudge
	if params.Fudge != 0 {
		fudgeEvery = params.XXXCount / params.Fudge
	}

	for e := range generateChan {
		var data []byte
		var err error

		switch params.Encode {
		case "json":
			data, err = json.Marshal(e)
		case "protobuf":
			data, err = proto.Marshal(e)
		}

		if err != nil {
			logrus.Errorf("unable to marshal event to %s: %s", params.Encode, err)
			logrus.Errorf("problem event: %+v", e)
			continue
		}

		if params.Fudge != 0 && params.Encode == "json" {
			iter += 1

			if iter == fudgeEvery {
				data, err = fudge(params, data)
				if err != nil {
					panic("unable to fudge: " + err.Error())
				}

				iter = 0
				numFudgedEvents += 1
			}
		}

		batch = append(batch, data)

		if len(batch) >= batchSize {
			logrus.Infof("%s: batch size reached (%d); sending events", id, len(batch))

			logrus.Infof("NOOP: Would have sent '%d' records", len(batch))

			if params.VerboseNoOp {
				for _, entry := range toGenericRecords(batch) {
					logrus.Infof("Message body: %s", string(entry.Body))
				}
			}

			numEvents += len(batch)

			performSleep(params)

			// Reset batch
			batch = make([][]byte, 0)

			// BatchSizeRandom batch size either up or down in size
			if params.BatchSizeRandom {
				randomizer := rand.New(rand.NewSource(time.Now().UnixNano()))

				var fudgeFactor int

				// Prevent a panic by never passing 0 to Intn()
				if params.BatchSize/5 != 0 {
					fudgeFactor = randomizer.Intn(params.BatchSize / 5)
				}

				if fudgeFactor%2 == 0 {
					logrus.Infof("Fudging batch-size UP by %d", fudgeFactor)
					batchSize = params.BatchSize + fudgeFactor
				} else {
					logrus.Infof("Fudging batch-size DOWN by %d", fudgeFactor)
					batchSize = params.BatchSize - fudgeFactor
				}
			}
		}
	}

	logrus.Infof("%s: sending final batch (length: %d)", id, len(batch))

	if params.VerboseNoOp {
		for _, entry := range toGenericRecords(batch) {
			logrus.Infof("Message body: %s", string(entry.Body))
		}
	}

	numEvents += len(batch)

	logrus.Infof("%s: finished work (sent: %d, fudged: %d); exiting", id, numEvents, numFudgedEvents)
}
