package params

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/batchcorp/event-generator/params/types"
)

func GetIntegerRange(input string) (*types.IntegerRange, error) {
	if input == "" {
		return nil, errors.New("count cannot be empty")
	}

	if !strings.Contains(input, ":") {
		count, err := strconv.Atoi(input)
		if err != nil {
			return nil, fmt.Errorf("unable to convert count to int: %s", err)
		}

		return &types.IntegerRange{
			Value: count,
		}, nil
	}

	// Input is a range
	countRange := strings.Split(input, ":")

	if len(countRange) != 2 {
		return nil, fmt.Errorf("unable to parse count range '%s'", input)
	}

	min := countRange[0]
	max := countRange[1]

	minCount, err := strconv.Atoi(min)
	if err != nil {
		return nil, fmt.Errorf("unable to convert min count to int: %s", err)
	}

	maxCount, err := strconv.Atoi(max)
	if err != nil {
		return nil, fmt.Errorf("unable to convert max count to int: %s", err)
	}

	if minCount > maxCount {
		return nil, errors.New("min count cannot be greater than max count")
	}

	return &types.IntegerRange{
		Value: rand.Intn(maxCount - minCount + 1),
		Min:   minCount,
		Max:   maxCount,
	}, nil
}

func GetIntervalRange(input string) (*types.IntervalRange, error) {
	if !strings.Contains(input, ":") {
		// IntervalRange interval is a single time.Duration
		interval, err := time.ParseDuration(input)
		if err != nil {
			return nil, fmt.Errorf("unable to parse continuous interval: %s", err)
		}

		return &types.IntervalRange{
			Value: interval,
		}, nil
	}

	// IntervalRange interval is a range
	intervalRange := strings.Split(input, ":")

	if len(intervalRange) != 2 {
		return nil, fmt.Errorf("unable to parse continuous interval range '%s'", input)
	}

	min := intervalRange[0]
	max := intervalRange[1]

	minInterval, err := time.ParseDuration(min)
	if err != nil {
		return nil, fmt.Errorf("unable to parse min continuous interval: %s", err)
	}

	maxInterval, err := time.ParseDuration(max)
	if err != nil {
		return nil, fmt.Errorf("unable to parse max continuous interval: %s", err)
	}

	if minInterval > maxInterval {
		return nil, fmt.Errorf("min continuous interval cannot be greater than max continuous interval")
	}

	// Generate random time.Duration between min and max
	rand.Seed(time.Now().UnixNano())

	return &types.IntervalRange{
		Value: time.Second * time.Duration(rand.Intn(int(maxInterval.Seconds())-int(minInterval.Seconds()))+1),
		Min:   minInterval,
		Max:   maxInterval,
	}, nil
}
