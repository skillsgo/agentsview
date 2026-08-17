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
	contentCodecRaw         = 0
	contentCodecZstd        = 1
	contentRefCountStatsKey = "agent_content_refcount_v1"
)

type agentContentReference struct {
	table   string
	columns []string
}

var agentContentReferences = []agentContentReference{
	{table: "messages", columns: []string{"content_object_id", "thinking_object_id"}},
	{table: "tool_calls", columns: []string{"input_object_id", "result_object_id"}},
	{table: "tool_result_events", columns: []string{"content_object_id"}},
}

// installAgentContentLifecycleLocked makes content object ownership exact.
// Reference counts avoid five reverse indexes and let deletes reclaim an
// object in O(1). The triggers are derived from the authoritative reference
// list so adding another Agent body cannot silently omit lifecycle handling.
func installAgentContentLifecycleLocked(w *writerHandle) error {
	for _, ref := range agentContentReferences {
		for _, event := range []string{"ai", "ad", "au"} {
			if _, err := w.Exec("DROP TRIGGER IF EXISTS content_ref_" + ref.table + "_" + event); err != nil {
				return fmt.Errorf("dropping Agent content lifecycle trigger: %w", err)
			}
		}
		var deleteBody strings.Builder
		for _, column := range ref.columns {
			if _, err := w.Exec("DROP TRIGGER IF EXISTS content_ref_" + ref.table + "_au_" + column); err != nil {
				return fmt.Errorf("dropping Agent content column trigger: %w", err)
			}
			fmt.Fprintf(&deleteBody, "UPDATE content_objects SET ref_count = ref_count - 1 WHERE id = OLD.%s;\n", column)
		}
		oldIDs := make([]string, 0, len(ref.columns))
		for _, column := range ref.columns {
			oldIDs = append(oldIDs, "OLD."+column)
		}
		prune := "DELETE FROM content_objects WHERE ref_count = 0 AND id IN (" + strings.Join(oldIDs, ",") + ");\n"
		if _, err := w.Exec(fmt.Sprintf(
			"CREATE TRIGGER content_ref_%s_ad AFTER DELETE ON %s BEGIN\n%s%sEND",
			ref.table, ref.table, deleteBody.String(), prune,
		)); err != nil {
			return fmt.Errorf("installing Agent content delete trigger: %w", err)
		}
		for _, column := range ref.columns {
			statement := fmt.Sprintf(
				"CREATE TRIGGER content_ref_%s_au_%s AFTER UPDATE OF %s ON %s BEGIN\n"+
					"UPDATE content_objects SET ref_count = ref_count - 1 WHERE id = OLD.%s;\n"+
					"DELETE FROM content_objects WHERE ref_count = 0 AND id = OLD.%s;\nEND",
				ref.table, column, column, ref.table, column, column,
			)
			if _, err := w.Exec(statement); err != nil {
				return fmt.Errorf("installing Agent content update trigger: %w", err)
			}
		}
	}

	var initialized int
	if err := w.QueryRow("SELECT count(*) FROM stats WHERE key = ?", contentRefCountStatsKey).Scan(&initialized); err != nil {
		return fmt.Errorf("checking Agent content reference counts: %w", err)
	}
	if initialized != 0 {
		return nil
	}
	if _, err := w.Exec(`UPDATE content_objects SET ref_count =
		(SELECT count(*) FROM messages WHERE content_object_id = content_objects.id) +
		(SELECT count(*) FROM messages WHERE thinking_object_id = content_objects.id) +
		(SELECT count(*) FROM tool_calls WHERE input_object_id = content_objects.id) +
		(SELECT count(*) FROM tool_calls WHERE result_object_id = content_objects.id) +
		(SELECT count(*) FROM tool_result_events WHERE content_object_id = content_objects.id)`); err != nil {
		return fmt.Errorf("backfilling Agent content reference counts: %w", err)
	}
	if _, err := w.Exec("DELETE FROM content_objects WHERE ref_count = 0"); err != nil {
		return fmt.Errorf("pruning unreferenced Agent content: %w", err)
	}
	if _, err := w.Exec("INSERT INTO stats(key, value) VALUES (?, '1')", contentRefCountStatsKey); err != nil {
		return fmt.Errorf("recording Agent content reference counts: %w", err)
	}
	return nil
}

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
			!message.IsSystem &&
				(message.Role == "user" || message.Role == "assistant") &&
				!IsSystemPrefixed(message.Content, message.Role),
		); err != nil {
			return fmt.Errorf("storing message content: %w", err)
		}
		if message.thinkingObjectID, err = putAgentContentTx(
			tx, encoder, message.ThinkingText, false,
		); err != nil {
			return fmt.Errorf("storing message thinking: %w", err)
		}
		for callIndex := range message.ToolCalls {
			call := &message.ToolCalls[callIndex]
			if call.inputObjectID, err = putAgentContentTx(
				tx, encoder, call.InputJSON, true,
			); err != nil {
				return fmt.Errorf("storing tool input: %w", err)
			}
			if call.resultObjectID, err = putAgentContentTx(
				tx, encoder, call.ResultContent, false,
			); err != nil {
				return fmt.Errorf("storing tool result: %w", err)
			}
			for eventIndex := range call.ResultEvents {
				event := &call.ResultEvents[eventIndex]
				if event.contentObjectID, err = putAgentContentTx(
					tx, encoder, event.Content, false,
				); err != nil {
					return fmt.Errorf("storing tool result event: %w", err)
				}
			}
		}
	}
	return nil
}

func putAgentContentTx(
	tx *sql.Tx, encoder *zstd.Encoder, content string, searchable bool,
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
		if _, err := tx.Exec(
			"UPDATE content_objects SET ref_count = ref_count + 1 WHERE id = ?", id,
		); err != nil {
			return nil, fmt.Errorf("reserving Agent content reference: %w", err)
		}
		if searchable {
			if _, err := tx.Exec(
				"UPDATE content_objects SET searchable = 1 WHERE id = ?", id,
			); err != nil {
				return nil, fmt.Errorf("marking Agent content searchable: %w", err)
			}
			if err := projectAgentContentTx(tx, id, content); err != nil {
				return nil, err
			}
		}
		return &id, nil
	}
	if err != sql.ErrNoRows {
		return nil, fmt.Errorf("looking up Agent content: %w", err)
	}
	codec, payload := encodeContent(encoder, raw)
	result, err := tx.Exec(`INSERT OR IGNORE INTO content_objects
		(digest, raw_size, codec, payload, ref_count, searchable)
		VALUES (?, ?, ?, ?, 1, ?)`, digest[:], len(raw), codec, payload, searchable)
	if err != nil {
		return nil, fmt.Errorf("inserting Agent content: %w", err)
	}
	if inserted, rowsErr := result.RowsAffected(); rowsErr == nil && inserted == 1 {
		id, err = result.LastInsertId()
		if err != nil {
			return nil, fmt.Errorf("reading Agent content id: %w", err)
		}
		if searchable {
			if err := projectAgentContentTx(tx, id, content); err != nil {
				return nil, err
			}
		}
		return &id, nil
	}
	if err := tx.QueryRow(
		"SELECT id FROM content_objects WHERE digest = ?", digest[:],
	).Scan(&id); err != nil {
		return nil, fmt.Errorf("resolving concurrent Agent content: %w", err)
	}
	if _, err := tx.Exec(
		"UPDATE content_objects SET ref_count = ref_count + 1, searchable = max(searchable, ?) WHERE id = ?",
		searchable, id,
	); err != nil {
		return nil, fmt.Errorf("reserving concurrent Agent content reference: %w", err)
	}
	return &id, nil
}

func projectAgentContentTx(tx *sql.Tx, id int64, content string) error {
	var attached int
	if err := tx.QueryRow(`SELECT count(*) FROM pragma_database_list
		WHERE name = 'search_index'`).Scan(&attached); err != nil {
		return fmt.Errorf("probing Agent content projection database: %w", err)
	}
	if attached == 0 {
		return nil
	}
	var exists int
	if err := tx.QueryRow(`SELECT count(*) FROM search_index.sqlite_schema
		WHERE type = 'table' AND name = 'content_fts'`).Scan(&exists); err != nil {
		return fmt.Errorf("probing Agent content projection: %w", err)
	}
	if exists == 0 {
		return nil
	}
	if _, err := tx.Exec(
		"INSERT OR REPLACE INTO search_index.content_fts(rowid, content) VALUES (?, ?)",
		id, content,
	); err != nil {
		return fmt.Errorf("projecting Agent content: %w", err)
	}
	return nil
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
