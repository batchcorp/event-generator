package generator

import (
	"math/rand"
	"time"

	uuid "github.com/satori/go.uuid"
)

var (
	// We do this to have some similarity in the data
	UserIDs = generateUUIDs(100)
	TeamIDs = generateUUIDs(100)
)

func generateUUIDs(count int) []string {
	ids := make([]string, 0)

	for i := 0; i != count; i++ {
		ids = append(ids, uuid.NewV4().String())
	}

	return ids
}

func RandomTeamID() string {
	rand.Seed(time.Now().UnixNano())

	return TeamIDs[rand.Intn(len(TeamIDs))]
}

func RandomUserID() string {
	rand.Seed(time.Now().UnixNano())

	return UserIDs[rand.Intn(len(UserIDs))]

}
