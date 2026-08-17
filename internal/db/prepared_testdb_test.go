package db

import (
	"crypto/rand"
	"database/sql"
	"fmt"
)

// OpenPreparedTestDB opens a private test database file that has already been
// initialized with the current schema and data version. It is intentionally
// test-only so production code cannot bypass the normal open/migration path.
func OpenPreparedTestDB(path string) (*DB, error) {
	searchPath := searchDatabasePath(path)
	writer, err := sql.Open("sqlite3", makeDSN(path, false))
	if err != nil {
		return nil, fmt.Errorf("opening prepared test writer: %w", err)
	}
	writer.SetMaxOpenConns(1)
	if err := configureWAL(writer); err != nil {
		writer.Close()
		return nil, fmt.Errorf("configuring prepared test wal: %w", err)
	}
	if _, err := writer.Exec("ATTACH DATABASE ? AS search_index", searchPath); err != nil {
		writer.Close()
		return nil, fmt.Errorf("attaching prepared test search database: %w", err)
	}
	if _, err := writer.Exec(schemaFTS); err != nil {
		writer.Close()
		return nil, fmt.Errorf("initializing prepared test search database: %w", err)
	}

	reader, err := sql.Open(sqliteUsageDriverName, makeDSN(path, true))
	if err != nil {
		writer.Close()
		return nil, fmt.Errorf("opening prepared test reader: %w", err)
	}
	reader.SetMaxOpenConns(4)
	searchReader, err := openSearchReader(path, searchPath)
	if err != nil {
		writer.Close()
		reader.Close()
		return nil, err
	}

	db := &DB{path: path, searchPath: searchPath}
	db.writer.Store(writer)
	db.reader.Store(reader)
	db.searchReader.Store(searchReader)

	db.cursorSecret = make([]byte, 32)
	if _, err := rand.Read(db.cursorSecret); err != nil {
		writer.Close()
		reader.Close()
		return nil, fmt.Errorf(
			"generating prepared test cursor secret: %w", err,
		)
	}
	if err := db.ensureContentSearchProjection(); err != nil {
		db.Close()
		return nil, fmt.Errorf("rebuilding prepared test search index: %w", err)
	}

	db.startWALCheckpointLoop()
	return db, nil
}
