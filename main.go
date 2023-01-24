package main

import (
	"encoding/csv"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"

	"github.com/gogama/incite"

	"github.com/emirpasic/gods/sets/hashset"
)

const TimeChunk = 4 * time.Hour
const TimeJitter = 5 * time.Minute

func main() {
	now := time.Now()
	start := time.Date(2022, 10, 15, 0, 0, 0, 0, time.Local)
	end := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 0, time.Local) // midnight of current day

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

	// Use a set to dedup the stream IDs
	allStreamsSet := hashset.New()
	allStreamsSet.Add(findStreams(qp1, start, end))
	allStreamsSet.Add(findStreams(qp2, start, end))
	allStreamsSet.Add(findStreams(qp3, start, end))

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

func findStreams(queryPattern string, start, end time.Time) *hashset.Set {
	sess := session.Must(session.NewSession())
	a := cloudwatchlogs.New(sess)
	m := incite.NewQueryManager(incite.Config{Actions: a})
	defer func() {
		_ = m.Close()
	}()

	s := start
	e := s.Add(TimeChunk)

	// Use a set to dedup the stream IDs
	streamsSet := hashset.New()

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
			streamsSet.Add(record.StreamId)
		}
		s = e
		timeRemaining := end.Sub(e)
		if timeRemaining > TimeChunk {
			e = e.Add(TimeChunk)
			log.Printf("time remaining: %s", timeRemaining)
		} else if timeRemaining <= 0 {
			break
		} else {
			e = e.Add(timeRemaining)
		}
	}
	return streamsSet
}
