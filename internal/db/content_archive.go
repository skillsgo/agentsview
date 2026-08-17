package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	contentArchiveSchemaVersion = 1
	contentArchiveChunkSize     = 256 << 10
)

const contentArchiveSchema = `
CREATE TABLE metadata (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE content_objects (
    digest   BLOB PRIMARY KEY,
    raw_size INTEGER NOT NULL CHECK (raw_size > 0),
    chunks   INTEGER NOT NULL CHECK (chunks > 0)
) WITHOUT ROWID;
CREATE TABLE content_chunks (
    digest          BLOB PRIMARY KEY,
    raw_size        INTEGER NOT NULL CHECK (raw_size > 0),
    compressed_size INTEGER NOT NULL CHECK (compressed_size > 0),
    codec           TEXT NOT NULL,
    data            BLOB NOT NULL
) WITHOUT ROWID;
CREATE TABLE content_object_chunks (
    object_digest BLOB NOT NULL REFERENCES content_objects(digest),
    ordinal       INTEGER NOT NULL,
    chunk_digest  BLOB NOT NULL REFERENCES content_chunks(digest),
    PRIMARY KEY (object_digest, ordinal)
) WITHOUT ROWID;
CREATE TABLE content_refs (
    entity_kind     TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    message_ordinal INTEGER NOT NULL,
    call_index      INTEGER NOT NULL DEFAULT -1,
    event_index     INTEGER NOT NULL DEFAULT -1,
    role             TEXT NOT NULL DEFAULT '',
    field_kind       TEXT NOT NULL,
    content_digest  BLOB NOT NULL REFERENCES content_objects(digest),
    PRIMARY KEY (
        entity_kind, session_id, message_ordinal,
        call_index, event_index, field_kind
    )
) WITHOUT ROWID;
CREATE INDEX content_refs_digest ON content_refs(content_digest);
`

// ContentArchiveReport compares the legacy inline-content representation with
// the experimental Agent-aware content-addressed archive.
type ContentArchiveReport struct {
	SchemaVersion            int                          `json:"schemaVersion"`
	SourceBytes              int64                        `json:"sourceBytes"`
	ArchiveBytes             int64                        `json:"archiveBytes"`
	References               int64                        `json:"references"`
	UniqueObjects            int64                        `json:"uniqueObjects"`
	UniqueChunks             int64                        `json:"uniqueChunks"`
	ReferencedRawBytes       int64                        `json:"referencedRawBytes"`
	UniqueRawBytes           int64                        `json:"uniqueRawBytes"`
	CompressedChunkBytes     int64                        `json:"compressedChunkBytes"`
	DuplicateBytesEliminated int64                        `json:"duplicateBytesEliminated"`
	BuildDuration            time.Duration                `json:"buildDuration"`
	ByField                  map[string]ContentFieldStats `json:"byField"`
}

// ContentFieldStats measures one Agent-domain content role before global
// content identity and chunk compression are applied.
type ContentFieldStats struct {
	References int64 `json:"references"`
	RawBytes   int64 `json:"rawBytes"`
}

type contentReference struct {
	entityKind     string
	sessionID      string
	messageOrdinal int
	callIndex      int
	eventIndex     int
	role           string
	fieldKind      string
	content        string
}

type contentArchiveStatements struct {
	insertObject      *sql.Stmt
	insertChunk       *sql.Stmt
	insertObjectChunk *sql.Stmt
	insertReference   *sql.Stmt
}

// BuildContentArchive creates an immutable comparison archive beside the
// existing SQLite archive. The source is read-only and destination must not
// already exist.
func (db *DB) BuildContentArchive(
	ctx context.Context, destination string,
) (ContentArchiveReport, error) {
	started := time.Now()
	if destination == "" {
		return ContentArchiveReport{}, errors.New("content archive path is empty")
	}
	if _, err := os.Stat(destination); err == nil {
		return ContentArchiveReport{}, fmt.Errorf(
			"content archive already exists: %s", destination,
		)
	} else if !errors.Is(err, os.ErrNotExist) {
		return ContentArchiveReport{}, fmt.Errorf(
			"checking content archive destination: %w", err,
		)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return ContentArchiveReport{}, fmt.Errorf(
			"creating content archive directory: %w", err,
		)
	}
	temporary := fmt.Sprintf("%s.tmp-%d", destination, os.Getpid())
	_ = os.Remove(temporary)
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(temporary)
		}
	}()

	archive, err := sql.Open(sqliteDriverName, makeDSN(temporary, false))
	if err != nil {
		return ContentArchiveReport{}, fmt.Errorf("opening content archive: %w", err)
	}
	archive.SetMaxOpenConns(1)
	if err := initializeContentArchive(ctx, archive); err != nil {
		_ = archive.Close()
		return ContentArchiveReport{}, err
	}

	report := ContentArchiveReport{
		SchemaVersion: contentArchiveSchemaVersion,
		ByField:       make(map[string]ContentFieldStats),
	}
	if info, statErr := os.Stat(db.path); statErr == nil {
		report.SourceBytes = info.Size()
	}
	if err := db.populateContentArchive(ctx, archive, &report); err != nil {
		_ = archive.Close()
		return ContentArchiveReport{}, err
	}
	if err := archive.Close(); err != nil {
		return ContentArchiveReport{}, fmt.Errorf("closing content archive: %w", err)
	}
	if err := os.Chmod(temporary, 0o600); err != nil {
		return ContentArchiveReport{}, fmt.Errorf("protecting content archive: %w", err)
	}
	if err := os.Rename(temporary, destination); err != nil {
		return ContentArchiveReport{}, fmt.Errorf("publishing content archive: %w", err)
	}
	cleanup = false
	info, err := os.Stat(destination)
	if err != nil {
		return ContentArchiveReport{}, fmt.Errorf("measuring content archive: %w", err)
	}
	report.ArchiveBytes = info.Size()
	report.DuplicateBytesEliminated = report.ReferencedRawBytes - report.UniqueRawBytes
	report.BuildDuration = time.Since(started)
	return report, nil
}

func initializeContentArchive(ctx context.Context, archive *sql.DB) error {
	for _, pragma := range []string{
		"PRAGMA journal_mode=OFF",
		"PRAGMA synchronous=OFF",
		"PRAGMA temp_store=MEMORY",
		"PRAGMA foreign_keys=ON",
	} {
		if _, err := archive.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("configuring content archive: %w", err)
		}
	}
	if _, err := archive.ExecContext(ctx, contentArchiveSchema); err != nil {
		return fmt.Errorf("creating content archive schema: %w", err)
	}
	_, err := archive.ExecContext(ctx,
		"INSERT INTO metadata(key, value) VALUES ('schema_version', ?), ('chunk_size', ?), ('codec', 'zstd-default')",
		contentArchiveSchemaVersion, contentArchiveChunkSize,
	)
	if err != nil {
		return fmt.Errorf("writing content archive metadata: %w", err)
	}
	return nil
}

func (db *DB) populateContentArchive(
	ctx context.Context, archive *sql.DB, report *ContentArchiveReport,
) error {
	tx, err := archive.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("beginning content archive build: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	statements, err := prepareContentArchiveStatements(ctx, tx)
	if err != nil {
		return err
	}
	defer statements.close()
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(1),
	)
	if err != nil {
		return fmt.Errorf("creating zstd encoder: %w", err)
	}
	defer encoder.Close()

	queries := []string{
		`SELECT 'message', session_id, ordinal, -1, -1, role,
		        'message.content', content
		   FROM messages WHERE content != ''`,
		`SELECT 'message', session_id, ordinal, -1, -1, role,
		        'message.thinking', thinking_text
		   FROM messages WHERE thinking_text != ''`,
		`SELECT 'tool_call', tc.session_id, m.ordinal,
		        COALESCE(tc.call_index, -1), -1, '',
		        'tool_call.input', tc.input_json
		   FROM tool_calls tc JOIN messages m ON m.id = tc.message_id
		  WHERE COALESCE(tc.input_json, '') != ''`,
		`SELECT 'tool_call', tc.session_id, m.ordinal,
		        COALESCE(tc.call_index, -1), -1, '',
		        'tool_call.result', tc.result_content
		   FROM tool_calls tc JOIN messages m ON m.id = tc.message_id
		  WHERE COALESCE(tc.result_content, '') != ''`,
		`SELECT 'tool_result_event', session_id,
		        tool_call_message_ordinal, call_index, event_index, '',
		        'tool_result_event.content', content
		   FROM tool_result_events WHERE content != ''`,
	}
	seenObjects := make(map[[sha256.Size]byte]struct{})
	seenChunks := make(map[[sha256.Size]byte]struct{})
	for _, query := range queries {
		if err := db.streamContentReferences(ctx, query, func(ref contentReference) error {
			return storeContentReference(
				ctx, statements, encoder, ref, seenObjects, seenChunks, report,
			)
		}); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing content archive: %w", err)
	}
	return nil
}

func prepareContentArchiveStatements(
	ctx context.Context, tx *sql.Tx,
) (*contentArchiveStatements, error) {
	prepare := func(query string) (*sql.Stmt, error) {
		statement, err := tx.PrepareContext(ctx, query)
		if err != nil {
			return nil, err
		}
		return statement, nil
	}
	statements := &contentArchiveStatements{}
	var err error
	if statements.insertObject, err = prepare(
		"INSERT INTO content_objects(digest, raw_size, chunks) VALUES (?, ?, ?)",
	); err != nil {
		return nil, fmt.Errorf("preparing content object insert: %w", err)
	}
	if statements.insertChunk, err = prepare(
		"INSERT OR IGNORE INTO content_chunks(digest, raw_size, compressed_size, codec, data) VALUES (?, ?, ?, 'zstd-default', ?)",
	); err != nil {
		statements.close()
		return nil, fmt.Errorf("preparing content chunk insert: %w", err)
	}
	if statements.insertObjectChunk, err = prepare(
		"INSERT INTO content_object_chunks(object_digest, ordinal, chunk_digest) VALUES (?, ?, ?)",
	); err != nil {
		statements.close()
		return nil, fmt.Errorf("preparing content object chunk insert: %w", err)
	}
	if statements.insertReference, err = prepare(
		"INSERT INTO content_refs(entity_kind, session_id, message_ordinal, call_index, event_index, role, field_kind, content_digest) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
	); err != nil {
		statements.close()
		return nil, fmt.Errorf("preparing content reference insert: %w", err)
	}
	return statements, nil
}

func (s *contentArchiveStatements) close() {
	for _, statement := range []*sql.Stmt{
		s.insertObject, s.insertChunk, s.insertObjectChunk, s.insertReference,
	} {
		if statement != nil {
			_ = statement.Close()
		}
	}
}

func (db *DB) streamContentReferences(
	ctx context.Context, query string, consume func(contentReference) error,
) error {
	rows, err := db.getReader().QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("querying legacy content: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ref contentReference
		if err := rows.Scan(
			&ref.entityKind, &ref.sessionID, &ref.messageOrdinal,
			&ref.callIndex, &ref.eventIndex, &ref.role,
			&ref.fieldKind, &ref.content,
		); err != nil {
			return fmt.Errorf("scanning legacy content: %w", err)
		}
		if err := consume(ref); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("streaming legacy content: %w", err)
	}
	return nil
}

func storeContentReference(
	ctx context.Context,
	statements *contentArchiveStatements,
	encoder *zstd.Encoder,
	ref contentReference,
	seenObjects, seenChunks map[[sha256.Size]byte]struct{},
	report *ContentArchiveReport,
) error {
	raw := []byte(ref.content)
	objectDigest := sha256.Sum256(raw)
	report.References++
	report.ReferencedRawBytes += int64(len(raw))
	field := report.ByField[ref.fieldKind]
	field.References++
	field.RawBytes += int64(len(raw))
	report.ByField[ref.fieldKind] = field

	if _, exists := seenObjects[objectDigest]; !exists {
		chunks := (len(raw) + contentArchiveChunkSize - 1) / contentArchiveChunkSize
		if _, err := statements.insertObject.ExecContext(
			ctx, objectDigest[:], len(raw), chunks,
		); err != nil {
			return fmt.Errorf("inserting content object %s: %w",
				hex.EncodeToString(objectDigest[:8]), err)
		}
		seenObjects[objectDigest] = struct{}{}
		report.UniqueObjects++
		report.UniqueRawBytes += int64(len(raw))
		for ordinal, offset := 0, 0; offset < len(raw); ordinal, offset = ordinal+1, offset+contentArchiveChunkSize {
			end := min(offset+contentArchiveChunkSize, len(raw))
			chunk := raw[offset:end]
			chunkDigest := sha256.Sum256(chunk)
			if _, exists := seenChunks[chunkDigest]; !exists {
				compressed := encoder.EncodeAll(chunk, nil)
				if _, err := statements.insertChunk.ExecContext(
					ctx, chunkDigest[:], len(chunk), len(compressed), compressed,
				); err != nil {
					return fmt.Errorf("inserting content chunk: %w", err)
				}
				seenChunks[chunkDigest] = struct{}{}
				report.UniqueChunks++
				report.CompressedChunkBytes += int64(len(compressed))
			}
			if _, err := statements.insertObjectChunk.ExecContext(
				ctx, objectDigest[:], ordinal, chunkDigest[:],
			); err != nil {
				return fmt.Errorf("linking content chunk: %w", err)
			}
		}
	}
	if _, err := statements.insertReference.ExecContext(
		ctx, ref.entityKind, ref.sessionID, ref.messageOrdinal,
		ref.callIndex, ref.eventIndex, ref.role, ref.fieldKind, objectDigest[:],
	); err != nil {
		return fmt.Errorf("inserting %s content reference: %w", ref.fieldKind, err)
	}
	return nil
}

// VerifyContentArchive reconstructs every unique content object and verifies
// its raw length and SHA-256 identity.
func VerifyContentArchive(ctx context.Context, path string) error {
	archive, err := sql.Open(sqliteDriverName, makeDSN(path, true))
	if err != nil {
		return fmt.Errorf("opening content archive for verification: %w", err)
	}
	defer archive.Close()
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return fmt.Errorf("creating zstd decoder: %w", err)
	}
	defer decoder.Close()
	rows, err := archive.QueryContext(ctx,
		"SELECT digest, raw_size FROM content_objects ORDER BY digest",
	)
	if err != nil {
		return fmt.Errorf("querying content objects: %w", err)
	}
	type contentObject struct {
		digest  []byte
		rawSize int
	}
	var objects []contentObject
	for rows.Next() {
		var object contentObject
		if err := rows.Scan(&object.digest, &object.rawSize); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scanning content object: %w", err)
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("streaming content objects: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("closing content objects: %w", err)
	}
	for _, object := range objects {
		chunks, err := archive.QueryContext(ctx, `
			SELECT c.data
			  FROM content_object_chunks oc
			  JOIN content_chunks c ON c.digest = oc.chunk_digest
			 WHERE oc.object_digest = ? ORDER BY oc.ordinal`, object.digest)
		if err != nil {
			return fmt.Errorf("querying content chunks: %w", err)
		}
		hash := sha256.New()
		decodedSize := 0
		for chunks.Next() {
			var compressed []byte
			if err := chunks.Scan(&compressed); err != nil {
				_ = chunks.Close()
				return fmt.Errorf("scanning content chunk: %w", err)
			}
			decoded, err := decoder.DecodeAll(compressed, nil)
			if err != nil {
				_ = chunks.Close()
				return fmt.Errorf("decompressing content chunk: %w", err)
			}
			decodedSize += len(decoded)
			_, _ = hash.Write(decoded)
		}
		if err := chunks.Close(); err != nil {
			return fmt.Errorf("closing content chunks: %w", err)
		}
		if decodedSize != object.rawSize ||
			!equalDigest(hash.Sum(nil), object.digest) {
			return fmt.Errorf("content object verification failed for %s",
				hex.EncodeToString(object.digest[:min(8, len(object.digest))]))
		}
	}
	return nil
}

func equalDigest(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var different byte
	for i := range left {
		different |= left[i] ^ right[i]
	}
	return different == 0
}
