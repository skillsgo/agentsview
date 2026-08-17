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
	contentArchiveSchemaVersion = 2
	contentArchiveChunkSize     = 256 << 10
	contentArchivePackSize      = 4 << 20
	contentArchivePageSize      = 4 << 10
)

const contentArchiveSchema = `
CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE content_packs (id INTEGER PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE content_objects (
    id INTEGER PRIMARY KEY,
    digest BLOB NOT NULL UNIQUE,
    raw_size INTEGER NOT NULL CHECK (raw_size > 0),
    chunks INTEGER NOT NULL CHECK (chunks > 0),
    codec INTEGER NOT NULL,
    pack_id INTEGER,
    pack_offset INTEGER,
    compressed_size INTEGER,
    CHECK ((chunks = 1) = (pack_id IS NOT NULL))
);
CREATE TABLE content_chunks (
    id INTEGER PRIMARY KEY,
    digest BLOB NOT NULL UNIQUE,
    raw_size INTEGER NOT NULL CHECK (raw_size > 0),
    codec INTEGER NOT NULL,
    pack_id INTEGER NOT NULL,
    pack_offset INTEGER NOT NULL,
    compressed_size INTEGER NOT NULL CHECK (compressed_size > 0)
);
CREATE TABLE content_object_chunks (
    object_id INTEGER NOT NULL REFERENCES content_objects(id),
    ordinal INTEGER NOT NULL,
    chunk_id INTEGER NOT NULL REFERENCES content_chunks(id),
    PRIMARY KEY (object_id, ordinal)
) WITHOUT ROWID;
CREATE TABLE content_refs (
    entity_kind INTEGER NOT NULL,
    session_id TEXT NOT NULL,
    message_ordinal INTEGER NOT NULL,
    call_index INTEGER NOT NULL DEFAULT -1,
    event_index INTEGER NOT NULL DEFAULT -1,
    role INTEGER NOT NULL DEFAULT 0,
    field_kind INTEGER NOT NULL,
    object_id INTEGER NOT NULL REFERENCES content_objects(id),
    PRIMARY KEY (entity_kind, session_id, message_ordinal,
                 call_index, event_index, field_kind)
) WITHOUT ROWID;
`

const (
	contentCodecRaw  = 0
	contentCodecZstd = 1
)

var entityKindCode = map[string]int{
	"message": 1, "tool_call": 2, "tool_result_event": 3,
}
var roleCode = map[string]int{
	"": 0, "system": 1, "user": 2, "assistant": 3, "tool": 4,
}
var fieldKindCode = map[string]int{
	"message.content": 1, "message.thinking": 2,
	"tool_call.input": 3, "tool_call.result": 4,
	"tool_result_event.content": 5,
}

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
	insertObject, insertChunk, insertObjectChunk *sql.Stmt
	insertReference, insertPack, updatePack      *sql.Stmt
}

type contentPackWriter struct {
	ctx        context.Context
	statements *contentArchiveStatements
	id         int64
	data       []byte
}

func (w *contentPackWriter) add(data []byte) (int64, int, error) {
	if w.id != 0 && len(w.data) > 0 && len(w.data)+len(data) > contentArchivePackSize {
		if err := w.flush(); err != nil {
			return 0, 0, err
		}
	}
	if w.id == 0 {
		result, err := w.statements.insertPack.ExecContext(w.ctx)
		if err != nil {
			return 0, 0, fmt.Errorf("starting content pack: %w", err)
		}
		w.id, err = result.LastInsertId()
		if err != nil {
			return 0, 0, fmt.Errorf("reading content pack id: %w", err)
		}
	}
	offset := len(w.data)
	w.data = append(w.data, data...)
	return w.id, offset, nil
}

func (w *contentPackWriter) flush() error {
	if w.id == 0 {
		return nil
	}
	if _, err := w.statements.updatePack.ExecContext(w.ctx, w.data, w.id); err != nil {
		return fmt.Errorf("publishing content pack: %w", err)
	}
	w.id = 0
	w.data = w.data[:0]
	return nil
}

// BuildContentArchive creates an immutable comparison archive. The source is
// held in one read snapshot and destination must not already exist.
func (db *DB) BuildContentArchive(ctx context.Context, destination string) (ContentArchiveReport, error) {
	started := time.Now()
	if destination == "" {
		return ContentArchiveReport{}, errors.New("content archive path is empty")
	}
	if _, err := os.Stat(destination); err == nil {
		return ContentArchiveReport{}, fmt.Errorf("content archive already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return ContentArchiveReport{}, fmt.Errorf("checking content archive destination: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return ContentArchiveReport{}, fmt.Errorf("creating content archive directory: %w", err)
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
	report := ContentArchiveReport{SchemaVersion: contentArchiveSchemaVersion, ByField: make(map[string]ContentFieldStats)}
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
		fmt.Sprintf("PRAGMA page_size=%d", contentArchivePageSize),
		"PRAGMA journal_mode=OFF", "PRAGMA synchronous=OFF",
		"PRAGMA temp_store=MEMORY", "PRAGMA foreign_keys=ON",
	} {
		if _, err := archive.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("configuring content archive: %w", err)
		}
	}
	if _, err := archive.ExecContext(ctx, contentArchiveSchema); err != nil {
		return fmt.Errorf("creating content archive schema: %w", err)
	}
	_, err := archive.ExecContext(ctx, `INSERT INTO metadata(key, value) VALUES
		('schema_version', ?), ('chunk_size', ?), ('pack_size', ?),
		('page_size', ?), ('codec', 'zstd-best-or-raw')`,
		contentArchiveSchemaVersion, contentArchiveChunkSize,
		contentArchivePackSize, contentArchivePageSize)
	if err != nil {
		return fmt.Errorf("writing content archive metadata: %w", err)
	}
	return nil
}

func (db *DB) populateContentArchive(ctx context.Context, archive *sql.DB, report *ContentArchiveReport) error {
	sourceTx, err := db.getReader().BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("beginning source archive snapshot: %w", err)
	}
	defer func() { _ = sourceTx.Rollback() }()
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
		zstd.WithEncoderLevel(zstd.SpeedBestCompression),
		zstd.WithEncoderConcurrency(1))
	if err != nil {
		return fmt.Errorf("creating zstd encoder: %w", err)
	}
	defer encoder.Close()
	packer := &contentPackWriter{ctx: ctx, statements: statements}
	queries := []string{
		`SELECT 'message', session_id, ordinal, -1, -1, role, 'message.content', content FROM messages WHERE content != ''`,
		`SELECT 'message', session_id, ordinal, -1, -1, role, 'message.thinking', thinking_text FROM messages WHERE thinking_text != ''`,
		`SELECT 'tool_call', tc.session_id, m.ordinal, COALESCE(tc.call_index, -1), -1, '', 'tool_call.input', tc.input_json FROM tool_calls tc JOIN messages m ON m.id = tc.message_id WHERE COALESCE(tc.input_json, '') != ''`,
		`SELECT 'tool_call', tc.session_id, m.ordinal, COALESCE(tc.call_index, -1), -1, '', 'tool_call.result', tc.result_content FROM tool_calls tc JOIN messages m ON m.id = tc.message_id WHERE COALESCE(tc.result_content, '') != ''`,
		`SELECT 'tool_result_event', session_id, tool_call_message_ordinal, call_index, event_index, '', 'tool_result_event.content', content FROM tool_result_events WHERE content != ''`,
	}
	seenObjects := make(map[[sha256.Size]byte]int64)
	seenChunks := make(map[[sha256.Size]byte]int64)
	for _, query := range queries {
		if err := streamContentReferences(ctx, sourceTx, query, func(ref contentReference) error {
			return storeContentReference(ctx, statements, packer, encoder, ref, seenObjects, seenChunks, report)
		}); err != nil {
			return err
		}
	}
	if err := packer.flush(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("committing content archive: %w", err)
	}
	if err := sourceTx.Commit(); err != nil {
		return fmt.Errorf("closing source archive snapshot: %w", err)
	}
	return nil
}

func prepareContentArchiveStatements(ctx context.Context, tx *sql.Tx) (*contentArchiveStatements, error) {
	prepare := func(query string) (*sql.Stmt, error) { return tx.PrepareContext(ctx, query) }
	s := &contentArchiveStatements{}
	var err error
	if s.insertObject, err = prepare(`INSERT INTO content_objects (digest, raw_size, chunks, codec, pack_id, pack_offset, compressed_size) VALUES (?, ?, ?, ?, ?, ?, ?)`); err != nil {
		return nil, fmt.Errorf("preparing content object insert: %w", err)
	}
	if s.insertChunk, err = prepare(`INSERT INTO content_chunks (digest, raw_size, codec, pack_id, pack_offset, compressed_size) VALUES (?, ?, ?, ?, ?, ?)`); err != nil {
		s.close()
		return nil, fmt.Errorf("preparing content chunk insert: %w", err)
	}
	if s.insertObjectChunk, err = prepare("INSERT INTO content_object_chunks(object_id, ordinal, chunk_id) VALUES (?, ?, ?)"); err != nil {
		s.close()
		return nil, fmt.Errorf("preparing content object chunk insert: %w", err)
	}
	if s.insertReference, err = prepare(`INSERT INTO content_refs (entity_kind, session_id, message_ordinal, call_index, event_index, role, field_kind, object_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`); err != nil {
		s.close()
		return nil, fmt.Errorf("preparing content reference insert: %w", err)
	}
	if s.insertPack, err = prepare("INSERT INTO content_packs(data) VALUES (x'')"); err != nil {
		s.close()
		return nil, fmt.Errorf("preparing content pack insert: %w", err)
	}
	if s.updatePack, err = prepare("UPDATE content_packs SET data = ? WHERE id = ?"); err != nil {
		s.close()
		return nil, fmt.Errorf("preparing content pack update: %w", err)
	}
	return s, nil
}

func (s *contentArchiveStatements) close() {
	for _, statement := range []*sql.Stmt{s.insertObject, s.insertChunk, s.insertObjectChunk, s.insertReference, s.insertPack, s.updatePack} {
		if statement != nil {
			_ = statement.Close()
		}
	}
}

func streamContentReferences(ctx context.Context, source *sql.Tx, query string, consume func(contentReference) error) error {
	rows, err := source.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("querying legacy content: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var ref contentReference
		if err := rows.Scan(&ref.entityKind, &ref.sessionID, &ref.messageOrdinal, &ref.callIndex, &ref.eventIndex, &ref.role, &ref.fieldKind, &ref.content); err != nil {
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

func encodeContent(encoder *zstd.Encoder, raw []byte) (int, []byte) {
	compressed := encoder.EncodeAll(raw, nil)
	if len(compressed) >= len(raw) {
		return contentCodecRaw, raw
	}
	return contentCodecZstd, compressed
}

func storeContentReference(ctx context.Context, s *contentArchiveStatements, packer *contentPackWriter,
	encoder *zstd.Encoder, ref contentReference, seenObjects, seenChunks map[[sha256.Size]byte]int64,
	report *ContentArchiveReport) error {
	raw := []byte(ref.content)
	digest := sha256.Sum256(raw)
	report.References++
	report.ReferencedRawBytes += int64(len(raw))
	field := report.ByField[ref.fieldKind]
	field.References++
	field.RawBytes += int64(len(raw))
	report.ByField[ref.fieldKind] = field
	objectID, exists := seenObjects[digest]
	if !exists {
		chunks := (len(raw) + contentArchiveChunkSize - 1) / contentArchiveChunkSize
		if chunks == 1 {
			codec, encoded := encodeContent(encoder, raw)
			packID, offset, err := packer.add(encoded)
			if err != nil {
				return err
			}
			result, err := s.insertObject.ExecContext(ctx, digest[:], len(raw), 1, codec, packID, offset, len(encoded))
			if err != nil {
				return fmt.Errorf("inserting content object %s: %w", hex.EncodeToString(digest[:8]), err)
			}
			objectID, err = result.LastInsertId()
			if err != nil {
				return fmt.Errorf("reading content object id: %w", err)
			}
			report.UniqueChunks++
			report.CompressedChunkBytes += int64(len(encoded))
		} else {
			result, err := s.insertObject.ExecContext(ctx, digest[:], len(raw), chunks, contentCodecZstd, nil, nil, nil)
			if err != nil {
				return fmt.Errorf("inserting content object %s: %w", hex.EncodeToString(digest[:8]), err)
			}
			objectID, err = result.LastInsertId()
			if err != nil {
				return fmt.Errorf("reading content object id: %w", err)
			}
			for ordinal, offset := 0, 0; offset < len(raw); ordinal, offset = ordinal+1, offset+contentArchiveChunkSize {
				end := min(offset+contentArchiveChunkSize, len(raw))
				chunk := raw[offset:end]
				chunkDigest := sha256.Sum256(chunk)
				chunkID, chunkExists := seenChunks[chunkDigest]
				if !chunkExists {
					codec, encoded := encodeContent(encoder, chunk)
					packID, packOffset, err := packer.add(encoded)
					if err != nil {
						return err
					}
					result, err := s.insertChunk.ExecContext(ctx, chunkDigest[:], len(chunk), codec, packID, packOffset, len(encoded))
					if err != nil {
						return fmt.Errorf("inserting content chunk: %w", err)
					}
					chunkID, err = result.LastInsertId()
					if err != nil {
						return fmt.Errorf("reading content chunk id: %w", err)
					}
					seenChunks[chunkDigest] = chunkID
					report.UniqueChunks++
					report.CompressedChunkBytes += int64(len(encoded))
				}
				if _, err := s.insertObjectChunk.ExecContext(ctx, objectID, ordinal, chunkID); err != nil {
					return fmt.Errorf("linking content chunk: %w", err)
				}
			}
		}
		seenObjects[digest] = objectID
		report.UniqueObjects++
		report.UniqueRawBytes += int64(len(raw))
	}
	entity, ok := entityKindCode[ref.entityKind]
	if !ok {
		return fmt.Errorf("unknown content entity kind %q", ref.entityKind)
	}
	role, ok := roleCode[ref.role]
	if !ok {
		return fmt.Errorf("unknown content role %q", ref.role)
	}
	fieldCode, ok := fieldKindCode[ref.fieldKind]
	if !ok {
		return fmt.Errorf("unknown content field kind %q", ref.fieldKind)
	}
	if _, err := s.insertReference.ExecContext(ctx, entity, ref.sessionID, ref.messageOrdinal, ref.callIndex, ref.eventIndex, role, fieldCode, objectID); err != nil {
		return fmt.Errorf("inserting %s content reference: %w", ref.fieldKind, err)
	}
	return nil
}

type contentPackReader struct {
	ctx     context.Context
	archive *sql.DB
	id      int64
	data    []byte
}

func (r *contentPackReader) slice(packID int64, offset, size int) ([]byte, error) {
	if packID != r.id {
		if err := r.archive.QueryRowContext(r.ctx, "SELECT data FROM content_packs WHERE id = ?", packID).Scan(&r.data); err != nil {
			return nil, fmt.Errorf("reading content pack %d: %w", packID, err)
		}
		r.id = packID
	}
	if offset < 0 || size <= 0 || offset > len(r.data)-size {
		return nil, fmt.Errorf("invalid content pack slice %d:%d+%d", packID, offset, size)
	}
	return r.data[offset : offset+size], nil
}

func decodeContent(decoder *zstd.Decoder, codec int, encoded []byte) ([]byte, error) {
	switch codec {
	case contentCodecRaw:
		return encoded, nil
	case contentCodecZstd:
		return decoder.DecodeAll(encoded, nil)
	default:
		return nil, fmt.Errorf("unknown content codec %d", codec)
	}
}

// VerifyContentArchive reconstructs every unique content object and verifies
// its raw length and SHA-256 identity.
func VerifyContentArchive(ctx context.Context, path string) error {
	archive, err := sql.Open(sqliteDriverName, makeDSN(path, true))
	if err != nil {
		return fmt.Errorf("opening content archive for verification: %w", err)
	}
	defer archive.Close()
	archive.SetMaxOpenConns(3)
	decoder, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		return fmt.Errorf("creating zstd decoder: %w", err)
	}
	defer decoder.Close()
	rows, err := archive.QueryContext(ctx, `SELECT id, digest, raw_size, chunks, codec,
		COALESCE(pack_id, 0), COALESCE(pack_offset, 0), COALESCE(compressed_size, 0)
		FROM content_objects ORDER BY id`)
	if err != nil {
		return fmt.Errorf("querying content objects: %w", err)
	}
	defer rows.Close()
	packReader := &contentPackReader{ctx: ctx, archive: archive}
	for rows.Next() {
		var id, packID int64
		var digest []byte
		var rawSize, chunks, codec, offset, encodedSize int
		if err := rows.Scan(&id, &digest, &rawSize, &chunks, &codec, &packID, &offset, &encodedSize); err != nil {
			return fmt.Errorf("scanning content object: %w", err)
		}
		hash := sha256.New()
		decodedSize := 0
		if chunks == 1 {
			encoded, err := packReader.slice(packID, offset, encodedSize)
			if err != nil {
				return err
			}
			decoded, err := decodeContent(decoder, codec, encoded)
			if err != nil {
				return fmt.Errorf("decompressing content object: %w", err)
			}
			decodedSize = len(decoded)
			_, _ = hash.Write(decoded)
		} else {
			chunkRows, err := archive.QueryContext(ctx, `SELECT c.codec, c.pack_id, c.pack_offset, c.compressed_size
				FROM content_object_chunks oc JOIN content_chunks c ON c.id = oc.chunk_id
				WHERE oc.object_id = ? ORDER BY oc.ordinal`, id)
			if err != nil {
				return fmt.Errorf("querying content chunks: %w", err)
			}
			for chunkRows.Next() {
				var chunkCodec, chunkOffset, chunkSize int
				var chunkPackID int64
				if err := chunkRows.Scan(&chunkCodec, &chunkPackID, &chunkOffset, &chunkSize); err != nil {
					_ = chunkRows.Close()
					return fmt.Errorf("scanning content chunk: %w", err)
				}
				encoded, err := packReader.slice(chunkPackID, chunkOffset, chunkSize)
				if err != nil {
					_ = chunkRows.Close()
					return err
				}
				decoded, err := decodeContent(decoder, chunkCodec, encoded)
				if err != nil {
					_ = chunkRows.Close()
					return fmt.Errorf("decompressing content chunk: %w", err)
				}
				decodedSize += len(decoded)
				_, _ = hash.Write(decoded)
			}
			if err := chunkRows.Close(); err != nil {
				return fmt.Errorf("closing content chunks: %w", err)
			}
		}
		if decodedSize != rawSize || !equalDigest(hash.Sum(nil), digest) {
			return fmt.Errorf("content object verification failed for %s", hex.EncodeToString(digest[:min(8, len(digest))]))
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("streaming content objects: %w", err)
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
