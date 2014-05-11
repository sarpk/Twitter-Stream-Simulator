package main

import (
	"github.com/sarpk/Twitter-Stream-Simulator/workerpackage"
	"log"
	"time"
	"math/rand"
)

func randInt(min int , max int) int {
    return min + rand.Intn(max-min)
}

func sampleSimulator(body []byte) {
	log.Printf(" Processing %s", body)
	randSec := time.Duration(randInt(10,90))
	time.Sleep( randSec * time.Millisecond)//simulate working
}


func main() {
	rand.Seed( time.Now().UTC().UnixNano())
	msgs := workerpackage.InitWorker()
	workerpackage.ListenForever(msgs, sampleSimulator)
}
