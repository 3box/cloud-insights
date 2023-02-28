package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
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

const CasUrl = "https://cas.3boxlabs.com"
const CasApiTimeout = 3 * time.Minute
const CasApiRateLimit = 25
const CasApiBurst = 10
const CasApiOutstanding = 100

type Record struct {
	StreamId string `incite:"@streamId"`
	Cid      string `incite:"@cid"`
}

func main() {
	start := time.UnixMilli(1677531762543)
	end := time.UnixMilli(1677557873853)

	qp := "filter @logStream like /cas_api/" +
		"| fields @timestamp, @message" +
		"| filter @message like /Creating request with streamId/" +
		"| parse @message /Creating request with streamId (?<@streamId>[^ ]+) and commit CID (?<@cid>[^ ]+) failed/" +
		"| display @streamId, @cid"
	records := findStreams(qp, start, end)

	file, err := os.OpenFile("./cids.csv", os.O_WRONLY|os.O_APPEND|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("error creating file: %v", err)
	}
	writer := csv.NewWriter(file)
	for _, record := range records {
		err = writer.Write([]string{record.StreamId, record.Cid})
		if err != nil {
			log.Fatalf("error writing to file: %v", err)
		}
	}
	writer.Flush()
}

func findStreams(queryPattern string, start, end time.Time) []Record {
	sess := session.Must(session.NewSession())
	a := cloudwatchlogs.New(sess)
	m := incite.NewQueryManager(incite.Config{Actions: a})
	defer func() {
		_ = m.Close()
	}()

	s := start
	e := s.Add(TimeChunk)
	records := make([]Record, 0, 0)

	// Set to dedup the CIDs
	var cids = hashset.New()

	// Rate limiter to protect the CAS
	rlOpts := ratelimiter.Opts{
		Limit:             CasApiRateLimit,
		Burst:             CasApiBurst,
		MaxQueueDepth:     CasApiOutstanding,
		FullQueueStrategy: ratelimiter.BlockWhenFull,
	}
	limiter := ratelimiter.New(rlOpts, func(ctx context.Context, record Record) (types.Nil, error) {
		return types.Nil{}, casRequest(ctx, record)
	})

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
		if len(recs) == incite.MaxLimit {
			log.Fatalf("too many results in chunk")
		}
		for _, record := range recs {
			if !cids.Contains(record.Cid) {
				cids.Add(record.Cid)
				if _, err = limiter.Submit(context.Background(), record); err != nil {
					log.Printf("cas: request failure %s, %s", record.StreamId, record.Cid)
				}
				records = append(records, record)
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
	return records
}

func casRequest(ctx context.Context, record Record) error {
	log.Printf("cas: %v", record)

	type anchorRequest struct {
		StreamId string `json:"streamId"`
		Cid      string `json:"cid"`
	}
	reqBody, err := json.Marshal(anchorRequest{
		StreamId: record.StreamId,
		Cid:      record.Cid,
	})
	if err != nil {
		log.Printf("cas: error creating request json: %v", err)
		return err
	}

	rCtx, rCancel := context.WithTimeout(ctx, CasApiTimeout)
	defer rCancel()

	req, err := http.NewRequestWithContext(rCtx, "POST", CasUrl+"/api/v0/requests", bytes.NewBuffer(reqBody))
	if err != nil {
		log.Printf("cas: error creating request: %v", err)
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("cas: error submitting request: %v", err)
		return err
	}
	defer resp.Body.Close()
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("cas: error reading response: %v", err)
		return err
	}
	if resp.StatusCode != http.StatusOK {
		log.Printf("cas: error in response: %v", resp.StatusCode)
		return errors.New("cas: error in response")
	}
	log.Printf("cas: resp=%+v", respBody)
	return nil
}
