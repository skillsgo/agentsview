package db

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"fmt"
	"strings"

	"github.com/klauspost/compress/zstd"
)

const (
	contentCodecRaw  = 0
	contentCodecZstd = 1
)

// prepareAgentContentRefsTx writes every non-empty Agent body into the
// content-addressed physical layer and attaches private locators to the
// normalized rows. Inline bodies remain populated during migration parity.
func prepareAgentContentRefsTx(tx *sql.Tx, messages []Message) error {
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedBestCompression),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return fmt.Errorf("creating Agent content encoder: %w", err)
	}
	defer encoder.Close()
	for messageIndex := range messages {
		message := &messages[messageIndex]
		if message.contentObjectID, err = putAgentContentTx(
			tx, encoder, message.Content,
		); err != nil {
			return fmt.Errorf("storing message content: %w", err)
		}
		if message.thinkingObjectID, err = putAgentContentTx(
			tx, encoder, message.ThinkingText,
		); err != nil {
			return fmt.Errorf("storing message thinking: %w", err)
		}
		for callIndex := range message.ToolCalls {
			call := &message.ToolCalls[callIndex]
			if call.inputObjectID, err = putAgentContentTx(
				tx, encoder, call.InputJSON,
			); err != nil {
				return fmt.Errorf("storing tool input: %w", err)
			}
			if call.resultObjectID, err = putAgentContentTx(
				tx, encoder, call.ResultContent,
			); err != nil {
				return fmt.Errorf("storing tool result: %w", err)
			}
			for eventIndex := range call.ResultEvents {
				event := &call.ResultEvents[eventIndex]
				if event.contentObjectID, err = putAgentContentTx(
					tx, encoder, event.Content,
				); err != nil {
					return fmt.Errorf("storing tool result event: %w", err)
				}
			}
		}
	}
	return nil
}

func putAgentContentTx(
	tx *sql.Tx, encoder *zstd.Encoder, content string,
) (*int64, error) {
	if content == "" {
		return nil, nil
	}
	raw := []byte(content)
	digest := sha256.Sum256(raw)
	var id int64
	err := tx.QueryRow(
		"SELECT id FROM content_objects WHERE digest = ?", digest[:],
	).Scan(&id)
	if err == nil {
		return &id, nil
	}
	if err != sql.ErrNoRows {
		return nil, fmt.Errorf("looking up Agent content: %w", err)
	}
	codec, payload := encodeContent(encoder, raw)
	result, err := tx.Exec(`INSERT OR IGNORE INTO content_objects
		(digest, raw_size, codec, payload) VALUES (?, ?, ?, ?)`,
		digest[:], len(raw), codec, payload)
	if err != nil {
		return nil, fmt.Errorf("inserting Agent content: %w", err)
	}
	if inserted, rowsErr := result.RowsAffected(); rowsErr == nil && inserted == 1 {
		id, err = result.LastInsertId()
		if err != nil {
			return nil, fmt.Errorf("reading Agent content id: %w", err)
		}
		return &id, nil
	}
	if err := tx.QueryRow(
		"SELECT id FROM content_objects WHERE digest = ?", digest[:],
	).Scan(&id); err != nil {
		return nil, fmt.Errorf("resolving concurrent Agent content: %w", err)
	}
	return &id, nil
}

func readAgentContent(
	ctx context.Context, queryRow func(string, ...any) rowScanner, id int64,
) (string, error) {
	var digest, payload []byte
	var rawSize, codec int
	if err := queryRow(`SELECT digest, raw_size, codec, payload
		FROM content_objects WHERE id = ?`, id).Scan(
		&digest, &rawSize, &codec, &payload,
	); err != nil {
		return "", fmt.Errorf("reading Agent content %d: %w", id, err)
	}
	raw, err := decodeAgentContentPayload(codec, payload)
	if err != nil {
		return "", fmt.Errorf("decoding Agent content %d: %w", id, err)
	}
	actual := sha256.Sum256(raw)
	if len(raw) != rawSize || len(digest) != sha256.Size ||
		subtle.ConstantTimeCompare(actual[:], digest) != 1 {
		return "", fmt.Errorf("Agent content %d failed integrity verification", id)
	}
	return string(raw), nil
}

func decodeAgentContentPayload(codec int, payload []byte) ([]byte, error) {
	switch codec {
	case contentCodecRaw:
		return payload, nil
	case contentCodecZstd:
		decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			return nil, fmt.Errorf("creating Agent content decoder: %w", err)
		}
		defer decoder.Close()
		return decoder.DecodeAll(payload, nil)
	default:
		return nil, fmt.Errorf("unknown Agent content codec %d", codec)
	}
}

func encodeContent(encoder *zstd.Encoder, raw []byte) (int, []byte) {
	compressed := encoder.EncodeAll(raw, nil)
	if len(compressed) >= len(raw) {
		return contentCodecRaw, raw
	}
	return contentCodecZstd, compressed
}

// loadAgentContents fetches and verifies a set of content objects in one
// query. Callers use it to hydrate normalized rows without N+1 reads.
func loadAgentContents(
	ctx context.Context, q messageRowsQuerier, ids []int64,
) (map[int64]string, error) {
	result := make(map[int64]string, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	unique := make(map[int64]struct{}, len(ids))
	args := make([]any, 0, len(ids))
	marks := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == 0 {
			continue
		}
		if _, exists := unique[id]; exists {
			continue
		}
		unique[id] = struct{}{}
		args = append(args, id)
		marks = append(marks, "?")
	}
	if len(args) == 0 {
		return result, nil
	}
	rows, err := q.QueryContext(ctx, `SELECT id, digest, raw_size, codec, payload
		FROM content_objects WHERE id IN (`+strings.Join(marks, ",")+`)`, args...)
	if err != nil {
		return nil, fmt.Errorf("querying Agent contents: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var digest, payload []byte
		var rawSize, codec int
		if err := rows.Scan(&id, &digest, &rawSize, &codec, &payload); err != nil {
			return nil, fmt.Errorf("scanning Agent content: %w", err)
		}
		raw, err := decodeAgentContentPayload(codec, payload)
		if err != nil {
			return nil, fmt.Errorf("decoding Agent content %d: %w", id, err)
		}
		actual := sha256.Sum256(raw)
		if len(raw) != rawSize || len(digest) != sha256.Size ||
			subtle.ConstantTimeCompare(actual[:], digest) != 1 {
			return nil, fmt.Errorf("Agent content %d failed integrity verification", id)
		}
		result[id] = string(raw)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("streaming Agent contents: %w", err)
	}
	if len(result) != len(unique) {
		return nil, fmt.Errorf("Agent content reference is missing")
	}
	return result, nil
}
