package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/batchcorp/schemas/build/go/events/fakes"
	"github.com/sirupsen/logrus"

	"github.com/batchcorp/event-generator/events"
	"github.com/batchcorp/event-generator/output"
	"github.com/batchcorp/event-generator/params"
	"github.com/batchcorp/event-generator/params/types"
)

func main() {
	for {
		p := &types.Params{}

		if err := params.HandleParams(p); err != nil {
			logrus.Fatalf("unable to handle flags: %s", err)
		}

		logrus.Infof("Type: %s Output: %s Count: %d Fudge: %d Batch Size: %d ",
			p.Type, p.Output, p.XXXCount, p.XXXFudgeCount, p.XXXBatchSize)

		generateChan, err := events.GenerateEvents(p)
		if err != nil {
			logrus.Fatalf("unable to generate events: %s", err)
		}

		// Set appropriate func
		var sendEventsFunc func(wg *sync.WaitGroup, p *types.Params, id string, generateChan chan *fakes.Event)

		switch p.Output {
		case params.OutputKafka:
			sendEventsFunc = output.SendKafkaEvents
		case params.OutputGRPCCollector:
			sendEventsFunc = output.SendGRPCEvents
		case params.OutputRabbitMQ:
			sendEventsFunc = output.SendRabbitMQEvents
		case params.OutputNoOp:
			sendEventsFunc = output.SendNoOpEvents
		default:
			logrus.Fatalf("unknown output flag '%s'", p.Output)
		}

		wg := &sync.WaitGroup{}

		for i := 0; i < p.Workers; i++ {
			workerID := fmt.Sprintf("worker-%d", i)

			logrus.Infof("Launching worker '%s'", workerID)

			wg.Add(1)

			//noinspection GoNilness
			go sendEventsFunc(wg, p, workerID, generateChan)
		}

		logrus.Info("Waiting on workers to finish...")

		wg.Wait()

		if p.XXXContinuous == 0 {
			break
		}

		logrus.Infof("(continuous mode) sleeping for '%s' before next send (CTRL-C to stop)", p.XXXContinuous)
		time.Sleep(p.XXXContinuous)
	}

	logrus.Info("All work completed")
}
