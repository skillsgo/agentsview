package db

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"unsafe"

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

const (
	agentContentCompressionWorkers  = 4
	agentContentEncoderPoolCapacity = 4
)

var agentContentEncoderPool = make(
	chan *zstd.Encoder, agentContentEncoderPoolCapacity,
)

func acquireAgentContentEncoder() (*zstd.Encoder, error) {
	select {
	case encoder := <-agentContentEncoderPool:
		return encoder, nil
	default:
	}
	return zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedBestCompression),
		zstd.WithEncoderConcurrency(1),
	)
}

func releaseAgentContentEncoder(encoder *zstd.Encoder) {
	if encoder == nil {
		return
	}
	select {
	case agentContentEncoderPool <- encoder:
	default:
		encoder.Close()
	}
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
	inputs := make([]agentContentInput, 0, len(messages)*2)
	for messageIndex := range messages {
		message := &messages[messageIndex]
		inputs = append(inputs,
			agentContentInput{
				content: message.Content,
				searchable: !message.IsSystem &&
					(message.Role == "user" || message.Role == "assistant") &&
					!IsSystemPrefixed(message.Content, message.Role),
				target: &message.contentObjectID,
			},
			agentContentInput{content: message.ThinkingText, target: &message.thinkingObjectID},
		)
		for callIndex := range message.ToolCalls {
			call := &message.ToolCalls[callIndex]
			inputs = append(inputs,
				agentContentInput{content: call.InputJSON, searchable: true, target: &call.inputObjectID},
				agentContentInput{content: call.ResultContent, target: &call.resultObjectID},
			)
			for eventIndex := range call.ResultEvents {
				event := &call.ResultEvents[eventIndex]
				inputs = append(inputs, agentContentInput{
					content: event.Content,
					target:  &event.contentObjectID,
				})
			}
		}
	}
	if err := putAgentContentsTx(tx, inputs); err != nil {
		return fmt.Errorf("storing Agent content batch: %w", err)
	}
	return nil
}

type agentContentInput struct {
	content    string
	searchable bool
	target     **int64
}

type agentContentObject struct {
	digest     [sha256.Size]byte
	raw        []byte
	content    string
	searchable bool
	references int
	targets    []**int64
	codec      int
	payload    []byte
}

func putAgentContentsTx(tx *sql.Tx, inputs []agentContentInput) error {
	objectsByDigest := make(map[[sha256.Size]byte]*agentContentObject, len(inputs))
	objects := make([]*agentContentObject, 0, len(inputs))
	for _, input := range inputs {
		if input.content == "" {
			continue
		}
		digest := sha256.Sum256(unsafe.Slice(
			unsafe.StringData(input.content), len(input.content),
		))
		object := objectsByDigest[digest]
		if object == nil {
			object = &agentContentObject{digest: digest, content: input.content}
			objectsByDigest[digest] = object
			objects = append(objects, object)
		}
		object.searchable = object.searchable || input.searchable
		object.references++
		object.targets = append(object.targets, input.target)
	}
	if len(objects) == 0 {
		return nil
	}
	selectObject, err := tx.Prepare(
		"SELECT id FROM content_objects WHERE digest = ?",
	)
	if err != nil {
		return fmt.Errorf("preparing Agent content lookup: %w", err)
	}
	defer selectObject.Close()
	reserveObject, err := tx.Prepare(`UPDATE content_objects
		SET ref_count = ref_count + ?, searchable = max(searchable, ?)
		WHERE id = ?`)
	if err != nil {
		return fmt.Errorf("preparing Agent content reservation: %w", err)
	}
	defer reserveObject.Close()
	insertObject, err := tx.Prepare(`INSERT OR IGNORE INTO content_objects
		(digest, raw_size, codec, payload, ref_count, searchable)
		VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("preparing Agent content insert: %w", err)
	}
	defer insertObject.Close()
	projectionAvailable, err := agentContentProjectionAvailableTx(tx)
	if err != nil {
		return err
	}
	var projectObject *sql.Stmt
	if projectionAvailable {
		projectObject, err = tx.Prepare(
			"INSERT OR REPLACE INTO search_index.content_fts(rowid, content) VALUES (?, ?)",
		)
		if err != nil {
			return fmt.Errorf("preparing Agent content projection: %w", err)
		}
		defer projectObject.Close()
	}

	missing := make([]*agentContentObject, 0, len(objects))
	for _, object := range objects {
		var id int64
		err := selectObject.QueryRow(object.digest[:]).Scan(&id)
		if err == nil {
			if _, err := reserveObject.Exec(
				object.references, object.searchable, id,
			); err != nil {
				return fmt.Errorf("reserving Agent content references: %w", err)
			}
			if object.searchable && projectObject != nil {
				if _, err := projectObject.Exec(id, object.content); err != nil {
					return fmt.Errorf("projecting existing Agent content: %w", err)
				}
			}
			assignAgentContentTargets(object.targets, id)
			continue
		}
		if err != sql.ErrNoRows {
			return fmt.Errorf("looking up Agent content: %w", err)
		}
		object.raw = []byte(object.content)
		missing = append(missing, object)
	}
	if err := compressAgentContentObjects(missing); err != nil {
		return err
	}
	for _, object := range missing {
		result, err := insertObject.Exec(object.digest[:], len(object.raw),
			object.codec, object.payload, object.references, object.searchable)
		if err != nil {
			return fmt.Errorf("inserting Agent content: %w", err)
		}
		var id int64
		if inserted, rowsErr := result.RowsAffected(); rowsErr == nil && inserted == 1 {
			id, err = result.LastInsertId()
		} else {
			err = selectObject.QueryRow(object.digest[:]).Scan(&id)
			if err == nil {
				_, err = reserveObject.Exec(object.references, object.searchable, id)
			}
		}
		if err != nil {
			return fmt.Errorf("resolving Agent content insert: %w", err)
		}
		if object.searchable && projectObject != nil {
			if _, err := projectObject.Exec(id, object.content); err != nil {
				return fmt.Errorf("projecting new Agent content: %w", err)
			}
		}
		assignAgentContentTargets(object.targets, id)
	}
	return nil
}

func compressAgentContentObjects(objects []*agentContentObject) error {
	if len(objects) == 0 {
		return nil
	}
	jobs := make(chan *agentContentObject, len(objects))
	errs := make(chan error, min(agentContentCompressionWorkers, len(objects)))
	var workers sync.WaitGroup
	for range min(agentContentCompressionWorkers, len(objects)) {
		workers.Go(func() {
			encoder, err := acquireAgentContentEncoder()
			if err != nil {
				errs <- fmt.Errorf("creating Agent content encoder: %w", err)
				return
			}
			defer releaseAgentContentEncoder(encoder)
			for object := range jobs {
				object.codec, object.payload = encodeContent(encoder, object.raw)
			}
		})
	}
	for _, object := range objects {
		jobs <- object
	}
	close(jobs)
	workers.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}

func assignAgentContentTargets(targets []**int64, id int64) {
	for _, target := range targets {
		assigned := id
		*target = &assigned
	}
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
	available, err := agentContentProjectionAvailableTx(tx)
	if err != nil || !available {
		return err
	}
	if _, err := tx.Exec(
		"INSERT OR REPLACE INTO search_index.content_fts(rowid, content) VALUES (?, ?)",
		id, content,
	); err != nil {
		return fmt.Errorf("projecting Agent content: %w", err)
	}
	return nil
}

func agentContentProjectionAvailableTx(tx *sql.Tx) (bool, error) {
	var attached int
	if err := tx.QueryRow(`SELECT count(*) FROM pragma_database_list
		WHERE name = 'search_index'`).Scan(&attached); err != nil {
		return false, fmt.Errorf("probing Agent content projection database: %w", err)
	}
	if attached == 0 {
		return false, nil
	}
	var exists int
	if err := tx.QueryRow(`SELECT count(*) FROM search_index.sqlite_schema
		WHERE type = 'table' AND name = 'content_fts'`).Scan(&exists); err != nil {
		return false, fmt.Errorf("probing Agent content projection: %w", err)
	}
	return exists != 0, nil
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
