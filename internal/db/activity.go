package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type SessionActivityBucket struct {
	StartTime      string `json:"start_time"`
	EndTime        string `json:"end_time"`
	UserCount      int    `json:"user_count"`
	AssistantCount int    `json:"assistant_count"`
	FirstOrdinal   *int   `json:"first_ordinal"`
}

type SessionActivityResponse struct {
	Buckets         []SessionActivityBucket `json:"buckets"`
	IntervalSeconds int64                   `json:"interval_seconds"`
	TotalMessages   int                     `json:"total_messages"`
}

var intervalSteps = []int64{
	60, 120, 300, 600, 900, 1800, 3600, 7200,
}

const maxBuckets = 50

func SnapInterval(durationSec int64) int64 {
	if durationSec <= 0 {
		return intervalSteps[0]
	}
	target := durationSec / 30
	if target <= intervalSteps[0] {
		return intervalSteps[0]
	}
	best := intervalSteps[0]
	bestDist := abs64(intervalSteps[0] - target)
	for _, step := range intervalSteps {
		distance := abs64(step - target)
		if distance < bestDist || (distance == bestDist && step > best) {
			bestDist = distance
			best = step
		}
	}
	if durationSec/best+1 > maxBuckets {
		best = (durationSec + maxBuckets - 2) / (maxBuckets - 1)
	}
	return best
}

func abs64(value int64) int64 {
	if value < 0 {
		return -value
	}
	return value
}

func (d *DB) GetSessionActivity(
	ctx context.Context, sessionID string,
) (*SessionActivityResponse, error) {
	return getSessionActivitySQLite(d, ctx, sessionID)
}

type activityMessage struct {
	ordinal   int
	role      string
	isSystem  bool
	timestamp sql.NullString
	objectID  sql.NullInt64
}

// getSessionActivitySQLite hydrates only the content objects needed for the
// system-prefix visibility rule, then performs timestamp bucketing in Go. It
// therefore behaves identically with and without FTS and never needs an
// uncompressed body on messages.
func getSessionActivitySQLite(
	d *DB, ctx context.Context, sessionID string,
) (*SessionActivityResponse, error) {
	rows, err := d.getReader().QueryContext(ctx, `SELECT ordinal, role,
		is_system, timestamp, content_object_id
		FROM messages WHERE session_id = ? ORDER BY ordinal`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("querying activity messages: %w", err)
	}
	var messages []activityMessage
	var objectIDs []int64
	for rows.Next() {
		var message activityMessage
		if err := rows.Scan(&message.ordinal, &message.role, &message.isSystem,
			&message.timestamp, &message.objectID); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scanning activity message: %w", err)
		}
		messages = append(messages, message)
		if !message.isSystem && message.objectID.Valid {
			objectIDs = append(objectIDs, message.objectID.Int64)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	contents, err := loadAgentContents(ctx, d.getReader(), objectIDs)
	if err != nil {
		return nil, err
	}

	type visibleMessage struct {
		ordinal int
		role    string
		epoch   float64
	}
	visible := make([]visibleMessage, 0, len(messages))
	var minEpoch, maxEpoch float64
	for _, message := range messages {
		if message.isSystem || !message.timestamp.Valid || message.timestamp.String == "" {
			continue
		}
		content := ""
		if message.objectID.Valid {
			content = contents[message.objectID.Int64]
		}
		if IsSystemPrefixed(content, message.role) {
			continue
		}
		stamp, err := parseActivityTimestamp(message.timestamp.String)
		if err != nil {
			continue
		}
		epoch := float64(stamp.Unix()) + float64(stamp.Nanosecond())/1e9
		if len(visible) == 0 || epoch < minEpoch {
			minEpoch = epoch
		}
		if len(visible) == 0 || epoch > maxEpoch {
			maxEpoch = epoch
		}
		visible = append(visible, visibleMessage{
			ordinal: message.ordinal, role: message.role, epoch: epoch,
		})
	}
	if len(visible) == 0 {
		return &SessionActivityResponse{
			Buckets: []SessionActivityBucket{}, TotalMessages: len(messages),
		}, nil
	}

	epochMin := int64(minEpoch)
	durationSec := int64(maxEpoch - minEpoch)
	interval := SnapInterval(durationSec)
	type bucketCount struct {
		user, assistant int
		firstOrdinal    int
		set             bool
	}
	populated := make(map[int]*bucketCount)
	maxIndex := 0
	for _, message := range visible {
		index := int((message.epoch - float64(epochMin)) / float64(interval))
		bucket := populated[index]
		if bucket == nil {
			bucket = &bucketCount{firstOrdinal: message.ordinal, set: true}
			populated[index] = bucket
		}
		if message.ordinal < bucket.firstOrdinal {
			bucket.firstOrdinal = message.ordinal
		}
		switch message.role {
		case "user":
			bucket.user++
		case "assistant":
			bucket.assistant++
		}
		if index > maxIndex {
			maxIndex = index
		}
	}

	buckets := make([]SessionActivityBucket, maxIndex+1)
	for index := range buckets {
		start := epochMin + int64(index)*interval
		buckets[index] = SessionActivityBucket{
			StartTime: time.Unix(start, 0).UTC().Format(time.RFC3339),
			EndTime:   time.Unix(start+interval, 0).UTC().Format(time.RFC3339),
		}
		if count := populated[index]; count != nil && count.set {
			buckets[index].UserCount = count.user
			buckets[index].AssistantCount = count.assistant
			ordinal := count.firstOrdinal
			buckets[index].FirstOrdinal = &ordinal
		}
	}
	return &SessionActivityResponse{
		Buckets: buckets, IntervalSeconds: interval, TotalMessages: len(messages),
	}, nil
}

func parseActivityTimestamp(value string) (time.Time, error) {
	if stamp, err := time.Parse(time.RFC3339Nano, value); err == nil {
		return stamp, nil
	}
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999",
	} {
		if stamp, err := time.Parse(layout, value); err == nil {
			return stamp, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid timestamp %q", value)
}
