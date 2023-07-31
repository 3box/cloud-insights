package main

import (
	"bufio"
	"encoding/csv"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"

	"github.com/gogama/incite"

	"github.com/emirpasic/gods/sets/hashset"
)

const TimeChunk = 8 * time.Hour
const TimeJitter = 10 * time.Second

type Record struct {
	StreamId string `incite:"@streamId"`
	Cid      string `incite:"@cid"`
}

func main() {
	start := time.UnixMilli(1673740800000) // January 15th 2023
	end := time.Now()

	qp := "filter @logStream like /cas_anchor/" +
		"| fields @timestamp, @message" +
		"| filter @message like /Created anchor commit/" +
		"| parse @message /Created anchor commit with CID (?<@cid>[^ ]+) for stream (?<@streamId>[^']+)/" +
		"| display @streamId, @cid"

	streamsFile, err := os.OpenFile("./streams-to-fix.txt", os.O_RDONLY, 0666)
	if err != nil {
		log.Fatalf("error creating file: %v", err)
	}
	defer streamsFile.Close()

	streamsReader := bufio.NewScanner(streamsFile)
	streamsReader.Split(bufio.ScanLines)
	streamsToFix := hashset.New()
	for streamsReader.Scan() {
		streamsToFix.Add(streamsReader.Text())
	}
	records := findCids(qp, streamsToFix, start, end)

	cidsFile, err := os.OpenFile("./cids.txt", os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("error creating file: %v", err)
	}
	defer cidsFile.Close()

	cidsWriter := csv.NewWriter(cidsFile)
	for _, record := range records {
		err = cidsWriter.Write([]string{record.StreamId, record.Cid})
		if err != nil {
			log.Fatalf("error writing to file: %v", err)
		}
	}
	cidsWriter.Flush()
}

func findCids(queryPattern string, streamsToFix *hashset.Set, start, end time.Time) []Record {
	sess := session.Must(session.NewSession())
	a := cloudwatchlogs.New(sess)
	m := incite.NewQueryManager(incite.Config{Actions: a})
	defer func() {
		_ = m.Close()
	}()

	chunk := TimeChunk
	s := start
	e := s.Add(chunk)
	records := make([]Record, 0, 0)

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
		var recs []Record
		err = incite.Unmarshal(data, &recs)
		if err != nil {
			log.Fatalf("error unmarshalling: %v", err)
		}
		numRecs := len(recs)
		if numRecs < (incite.MaxLimit / 10) {
			chunk *= 2
			e = s.Add(chunk)
			// Restart with twice the time range, so we can get more records in each API call.
			continue
		} else if numRecs == incite.MaxLimit {
			chunk /= 2
			e = s.Add(chunk)
			// Restart with half the time range, so we don't get as many records in each API call. If we've reached
			// 10K, we've likely missed some records due to the API's maximum limit and should retry with a smaller time
			// range.
			continue
		}
		for _, record := range recs {
			if streamsToFix.Contains(record.StreamId) {
				records = append(records, record)
			}
		}
		timeRemaining := end.Sub(e)
		log.Printf("start=%s, end=%s, remaining=%s", s, e, timeRemaining)
		s = e
		if timeRemaining > chunk {
			e = e.Add(chunk)
		} else if timeRemaining <= 0 {
			break
		} else {
			e = e.Add(timeRemaining)
		}
	}
	return records
}
