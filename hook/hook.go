package hook

import (
	"log"
	"sync"
)

var ShouldStop = false
var Dict = make(map[string]string)
var DupRoundCountDict = make(map[string]int)
var ShouldStopDict = make(map[string]bool)

var mu = sync.Mutex{}

func Hook(id string, ip string) bool {
	mu.Lock()
	defer mu.Unlock()
	var dupFound = false
	if _, ok := Dict[id]; !ok {
		log.Printf("Node ID %s, IP %s\n", id, ip)
		Dict[id] = ip

	} else {
		dupFound = true
	}
	return dupFound
}
