package main

import (
	"bufio"
	"context"
	"errors"
	"go/types"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/abevier/tsk/ratelimiter"

	"github.com/emirpasic/gods/sets/hashset"
)

const CeramicUrl = "https://ceramic-gitcoin.hirenodes.io"
const CeramicSyncTimeout = 180 * time.Second
const CeramicSyncRateLimit = 5
const CeramicSyncBurst = 5
const CeramicSyncOutstanding = 5

// Use a set to dedup the stream IDs
var allStreamsSet = hashset.New()
var limiter *ratelimiter.RateLimiter[string, types.Nil] = nil

func main() {
	rlOpts := ratelimiter.Opts{
		Limit:             CeramicSyncRateLimit,
		Burst:             CeramicSyncBurst,
		MaxQueueDepth:     CeramicSyncOutstanding,
		FullQueueStrategy: ratelimiter.BlockWhenFull,
	}
	limiter = ratelimiter.New(rlOpts, func(ctx context.Context, streamId string) (types.Nil, error) {
		return types.Nil{}, ceramicSync(ctx, streamId)
	})

	syncStreams()
}

func syncStreams() {
	file, err := os.Open("./streams.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		split := strings.Split(scanner.Text(), " ")
		streamId := split[len(split)-1]
		if !allStreamsSet.Contains(streamId) {
			allStreamsSet.Add(streamId)
			go func() {
				if _, err = limiter.Submit(context.Background(), streamId); err != nil {
					log.Printf("sync: failure syncing stream %s", streamId)
				}
			}()
		}
	}
	if err = scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func ceramicSync(ctx context.Context, streamId string) error {
	log.Printf("sync: %s", streamId)

	qCtx, qCancel := context.WithTimeout(ctx, CeramicSyncTimeout)
	defer qCancel()

	req, err := http.NewRequestWithContext(qCtx, "GET", CeramicUrl+"/api/v0/streams/"+streamId+"?sync=3", nil)
	if err != nil {
		log.Printf("sync: error creating stream request: %v", err)
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("sync: error submitting stream request: %v", err)
		return err
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("sync: error reading stream response: %v", err)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("error in query: %v, %s", resp.StatusCode, respBody)
		return errors.New("sync: error in response")
	}
	log.Printf("sync complete: %s", streamId)
	return nil
}
