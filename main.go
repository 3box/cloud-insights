package main

import (
	"context"
	"encoding/csv"
	"errors"
	"go/types"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"

	"github.com/gogama/incite"

	"github.com/abevier/tsk/ratelimiter"

	"github.com/emirpasic/gods/sets/hashset"
)

const TimeChunk = 4 * time.Hour
const TimeJitter = 5 * time.Minute

const CeramicUrl = "https://ceramic-private-cpc.3boxlabs.com"
const CeramicSyncTimeout = 60 * time.Second
const CeramicSyncRateLimit = 10
const CeramicSyncBurst = 5
const CeramicSyncOutstanding = 100

// Use a set to dedup the stream IDs
var allStreamsSet = hashset.New()
var limiter *ratelimiter.RateLimiter[string, types.Nil] = nil

func main() {
	now := time.Now()
	start := time.Date(2022, 10, 15, 0, 0, 0, 0, time.Local)
	end := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 0, time.Local) // midnight of current day

	rlOpts := ratelimiter.Opts{
		Limit:             CeramicSyncRateLimit,
		Burst:             CeramicSyncBurst,
		MaxQueueDepth:     CeramicSyncOutstanding,
		FullQueueStrategy: ratelimiter.BlockWhenFull,
	}
	limiter = ratelimiter.New(rlOpts, func(ctx context.Context, streamId string) (types.Nil, error) {
		return types.Nil{}, ceramicSync(ctx, streamId)
	})

	// Query patterns
	// [2023-01-24T15:36:15.535Z] WARNING: 'Error loading stream k2t6wyfsu4pfzxi96p62r7fdoyzk3g6syn8ur7pqe428ur9spnvg8agvbdt24b
	// at commit bagcqcera73kyqo74yjgtvmidn2sokfyg4atdfnqdsjt44ff3rbsktfi24snq at time undefined as part of a multiQuery
	// request: Error: Can not verify signature for commit bagcqcerab6hgmckxjx6mradg2hlhb2sujq2ekj7y3lz7twhxqclvs7slkuaq:
	// CACAO has expired'
	qp1 := "filter @logStream like /ceramic-node\\/ceramic_node\\//" +
		"| fields @message" +
		"| filter @message like /Error loading stream .* at commit .* CACAO has expired/" +
		"| parse @message /Error loading stream (?<@streamid>\\S+) at commit/" +
		"| display @streamid"
	// [2023-01-24T15:27:43.335Z] WARNING: 'Error while applying commit bagcqcerasyq5zpjfswc62suudfdmj552vuk77zfvwuc3o5sfqurwfzq3xota
	// to stream k2t6wyfsu4pfx15wjtw6o58xp14zjn75bmdse5y3s7o1ishr1reuwroilrp5d3: Error: Can not verify signature for commit
	// bagcqcerasyq5zpjfswc62suudfdmj552vuk77zfvwuc3o5sfqurwfzq3xota: CACAO has expired'
	qp2 := "filter @logStream like /ceramic-node\\/ceramic_node\\//" +
		"| fields @message" +
		"| filter @message like /Error while applying commit .* to stream .* CACAO has expired/" +
		"| parse @message /to stream (?<@streamid>\\S+):/" +
		"| display @streamid"
	// [2023-01-24T15:27:43.337Z] ERROR: Error: CACAO expired: Commit bagcqcerap5p4rharn7qv33cwjjra5z3xq2pijaoksbundldzauwjmvj5wgcq
	// of Stream k2t6wyfsu4pfx15wjtw6o58xp14zjn75bmdse5y3s7o1ishr1reuwroilrp5d3 has a CACAO that expired at 1673623133.
	// Loading the stream with 'sync: SyncOptions.ALWAYS_SYNC' will restore the stream to a usable state, by discarding
	// the invalid commits (this means losing the data from those invalid writes!)
	qp3 := "filter @logStream like /ceramic-node\\/ceramic_node\\//" +
		"| fields @message" +
		"| filter @message like /Error: CACAO expired: .* of Stream .* has a CACAO that expired .*/" +
		"| parse @message /of Stream (?<@streamid>\\S+)/" +
		"| display @streamid"

	findStreams(qp1, start, end)
	findStreams(qp2, start, end)
	findStreams(qp3, start, end)

	allStreamsFile, err := os.OpenFile("./streams_all.csv", os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("error creating file: %v", err)
	}
	allStreamsWriter := csv.NewWriter(allStreamsFile)
	for _, streamId := range allStreamsSet.Values() {
		err = allStreamsWriter.Write([]string{streamId.(string)})
		if err != nil {
			log.Fatalf("error writing to file: %v", err)
		}
	}
	allStreamsWriter.Flush()
	oldStreamsFile, err := os.OpenFile("./streams_old.csv", os.O_RDONLY, 0666)
	if err != nil {
		log.Fatalf("error reading file: %v", err)
	}
	oldStreamsReader := csv.NewReader(oldStreamsFile)
	oldStreamRecords, _ := oldStreamsReader.ReadAll()
	oldStreamsSet := hashset.New()
	for _, oldStreamRecord := range oldStreamRecords {
		oldStreamsSet.Add(oldStreamRecord[0])
	}
	newStreamsFile, err := os.OpenFile("./streams_new.csv", os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("error creating file: %v", err)
	}
	newStreamsWriter := csv.NewWriter(newStreamsFile)
	for _, streamId := range allStreamsSet.Values() {
		if !oldStreamsSet.Contains(streamId) {
			err = newStreamsWriter.Write([]string{streamId.(string)})
			if err != nil {
				log.Fatalf("error writing to file: %v", err)
			}
		} else {
			log.Printf("found old stream ID: %s", streamId)
		}
	}
	newStreamsWriter.Flush()
}

func findStreams(queryPattern string, start, end time.Time) {
	sess := session.Must(session.NewSession())
	a := cloudwatchlogs.New(sess)
	m := incite.NewQueryManager(incite.Config{Actions: a})
	defer func() {
		_ = m.Close()
	}()

	s := start
	e := s.Add(TimeChunk)

	for {
		query, err := m.Query(incite.QuerySpec{
			Text:   queryPattern,
			Start:  s.Add(-TimeJitter),
			End:    e.Add(TimeJitter),
			Groups: []string{"/ecs/ceramic-prod-cas"},
			Limit:  incite.MaxLimit,
		})
		if err != nil {
			log.Fatalf("error fetching: %v", err)
		}
		data, err := incite.ReadAll(query)
		if err != nil {
			log.Fatalf("error reading: %v", err)
		}
		var v []struct {
			StreamId string `incite:"@streamid"`
		}
		err = incite.Unmarshal(data, &v)
		if err != nil {
			log.Fatalf("error unmarshalling: %v", err)
		}
		if len(v) == incite.MaxLimit {
			log.Fatalf("too many results in chunk")
		}
		for _, record := range v {
			if !allStreamsSet.Contains(record.StreamId) {
				allStreamsSet.Add(record.StreamId)
				if _, err = limiter.Submit(context.Background(), record.StreamId); err != nil {
					log.Printf("sync: failure syncing stream %s", record.StreamId)
				}
			}
		}
		s = e
		timeRemaining := end.Sub(e)
		if timeRemaining > TimeChunk {
			log.Printf("qp=%s, end=%s, remaining=%s", queryPattern, e, timeRemaining)
			e = e.Add(TimeChunk)
		} else if timeRemaining <= 0 {
			break
		} else {
			e = e.Add(timeRemaining)
		}
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
	return nil
}
