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
	sess := session.Must(session.NewSession())
	a := cloudwatchlogs.New(sess)
	m := incite.NewQueryManager(incite.Config{Actions: a})
	defer func() {
		_ = m.Close()
	}()

	now := time.Now()
	start := time.Date(2022, 11, 22, 0, 0, 0, 0, time.Local)
	end := time.Date(now.Year(), now.Month(), now.Day(), 23, 59, 59, 0, time.Local) // midnight of current day
	s := start
	e := s.Add(TimeChunk)

	// Use a set to dedup the stream IDs
	allStreamsSet := hashset.New()

	for {
		query, err := m.Query(incite.QuerySpec{
			Text: "filter @logStream like /ceramic-node\\/ceramic_node\\//" +
				"| fields @message" +
				"| filter @message like /Error loading stream .* at commit .* CACAO has expired/" +
				"| parse @message /Error loading stream (?<@streamid>\\S+) at commit/" +
				"| display @streamid",
			Start:  s.Add(-TimeJitter),
			End:    e.Add(TimeJitter),
			Groups: []string{"/ecs/ceramic-prod-cas"},
			Limit:  incite.MaxLimit,
		})
		if err != nil {
			log.Fatalf("error fetching: %v", err)
			return
		}
		data, err := incite.ReadAll(query)
		if err != nil {
			log.Fatalf("error reading: %v", err)
			return
		}
		var v []struct {
			StreamId string `incite:"@streamid"`
		}
		err = incite.Unmarshal(data, &v)
		if err != nil {
			log.Fatalf("error unmarshalling: %v", err)
			return
		}
		if len(v) == incite.MaxLimit {
			log.Fatalf("too many results in chunk")
		}
		for _, record := range v {
			allStreamsSet.Add(record.StreamId)
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
