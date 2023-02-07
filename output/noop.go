package output

import (
	"encoding/json"
	"math/rand"
	"sync"
	"time"

	"github.com/batchcorp/schemas/build/go/events/fakes"
	"github.com/golang/protobuf/proto"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/event-generator/params/types"
)

func SendNoOpEvents(wg *sync.WaitGroup, p *types.Params, id string, generateChan chan *fakes.Event) {
	defer wg.Done()

	id = "noop-" + id

	logrus.Infof("worker id '%s' started", id)

	batch := make([][]byte, 0)

	batchSize := p.XXXBatchSize
	numEvents := 0
	iter := 0
	numFudgedEvents := 0
	fudgeEvery := 0

	// Figure out how often to fudge
	if p.XXXFudgeCount != 0 {
		fudgeEvery = p.XXXCount / p.XXXFudgeCount
	}

	for e := range generateChan {
		var data []byte
		var err error

		switch p.Encode {
		case "json":
			data, err = json.Marshal(e)
		case "protobuf":
			data, err = proto.Marshal(e)
		}

		if err != nil {
			logrus.Errorf("unable to marshal event to %s: %s", p.Encode, err)
			logrus.Errorf("problem event: %+v", e)
			continue
		}

		if p.XXXFudgeCountMax != 0 && p.Encode == "json" {
			iter += 1

			if iter == fudgeEvery {
				data, err = fudge(p, data)
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

			if p.Verbose {
				for _, entry := range toGenericRecords(batch, p.ForceDeadLetter) {
					logrus.Infof("Message body: %s", string(entry.Body))
				}
			}

			numEvents += len(batch)

			if p.XXXSleep > 0 {
				time.Sleep(p.XXXSleep)
			}

			// Reset batch
			batch = make([][]byte, 0)

			if p.XXXBatchSizeMin != 0 && p.XXXBatchSizeMax != 0 {
				randomizer := rand.New(rand.NewSource(time.Now().UnixNano()))
				batchSize = randomizer.Intn(p.XXXBatchSizeMax-p.XXXBatchSizeMin) + 1

				logrus.Infof("NOOP: Batch size randomized to '%d'", batchSize)
			}
		}
	}

	logrus.Infof("%s: sending final batch (length: %d)", id, len(batch))

	if p.Verbose {
		for _, entry := range toGenericRecords(batch, p.ForceDeadLetter) {
			logrus.Infof("Message body: %s", string(entry.Body))
		}
	}

	numEvents += len(batch)

	logrus.Infof("%s: finished work (sent: %d, fudged: %d); exiting", id, numEvents, numFudgedEvents)
}
