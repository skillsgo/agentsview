package db

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildContentArchiveDeduplicatesAcrossAgentRoles(t *testing.T) {
	database := testDB(t)
	insertSession(t, database, "session-a", "project")
	insertSession(t, database, "session-b", "project")

	sharedPrompt := "follow the repository instructions"
	sharedResult := "the command completed successfully"
	messages := []Message{
		{
			SessionID:    "session-a",
			Ordinal:      0,
			Role:         "assistant",
			Content:      sharedPrompt,
			ThinkingText: "inspect before editing",
			HasToolUse:   true,
			ToolCalls: []ToolCall{{
				SessionID:     "session-a",
				ToolName:      "Bash",
				ToolUseID:     "call-a",
				CallIndex:     0,
				InputJSON:     `{"cmd":"go test ./..."}`,
				ResultContent: sharedResult,
				ResultEvents: []ToolResultEvent{{
					ToolUseID:  "call-a",
					Source:     "tool_result",
					Status:     "completed",
					Content:    sharedResult,
					EventIndex: 0,
				}},
			}},
		},
		{
			SessionID: "session-b",
			Ordinal:   0,
			Role:      "user",
			Content:   sharedPrompt,
		},
	}
	require.NoError(t, database.InsertMessages(messages))

	destination := filepath.Join(t.TempDir(), "content.db")
	report, err := database.BuildContentArchive(context.Background(), destination)
	require.NoError(t, err)
	require.NoError(t, VerifyContentArchive(context.Background(), destination))

	assert.Equal(t, int64(6), report.References)
	assert.Equal(t, int64(4), report.UniqueObjects)
	assert.Equal(t,
		int64(len(sharedPrompt)+len(sharedResult)),
		report.DuplicateBytesEliminated,
	)
	assert.Equal(t, int64(2), report.ByField["message.content"].References)
	assert.Equal(t, int64(1), report.ByField["message.thinking"].References)
	assert.Positive(t, report.CompressedChunkBytes)
	assert.Positive(t, report.ArchiveBytes)

	info, err := os.Stat(destination)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	archive, err := sql.Open(sqliteDriverName, makeDSN(destination, true))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, archive.Close()) })
	var userRefs, assistantRefs int
	require.NoError(t, archive.QueryRow(
		"SELECT count(*) FROM content_refs WHERE role = ?", roleCode["user"],
	).Scan(&userRefs))
	require.NoError(t, archive.QueryRow(
		"SELECT count(*) FROM content_refs WHERE role = ?", roleCode["assistant"],
	).Scan(&assistantRefs))
	assert.Equal(t, 1, userRefs)
	assert.Equal(t, 2, assistantRefs)
}

func TestBuildContentArchiveRequiresFreshDestination(t *testing.T) {
	database := testDB(t)
	destination := filepath.Join(t.TempDir(), "existing.db")
	require.NoError(t, os.WriteFile(destination, []byte("owned"), 0o600))

	_, err := database.BuildContentArchive(context.Background(), destination)
	require.ErrorContains(t, err, "already exists")
	content, readErr := os.ReadFile(destination)
	require.NoError(t, readErr)
	assert.Equal(t, "owned", string(content))
}

func TestVerifyContentArchiveDetectsCorruption(t *testing.T) {
	database := testDB(t)
	insertSession(t, database, "session-a", "project")
	require.NoError(t, database.InsertMessages([]Message{{
		SessionID: "session-a",
		Ordinal:   0,
		Role:      "user",
		Content:   "content that must round trip",
	}}))
	destination := filepath.Join(t.TempDir(), "content.db")
	_, err := database.BuildContentArchive(context.Background(), destination)
	require.NoError(t, err)

	archive, err := sql.Open(sqliteDriverName, makeDSN(destination, false))
	require.NoError(t, err)
	_, err = archive.Exec("UPDATE content_packs SET data = x'00'")
	require.NoError(t, err)
	require.NoError(t, archive.Close())

	err = VerifyContentArchive(context.Background(), destination)
	require.Error(t, err)
}

func TestContentArchivePacksSingleChunkObjectsAndUsesEnums(t *testing.T) {
	database := testDB(t)
	insertSession(t, database, "session-a", "project")
	require.NoError(t, database.InsertMessages([]Message{{
		SessionID: "session-a", Ordinal: 0, Role: "user", Content: "small",
	}, {
		SessionID: "session-a", Ordinal: 1, Role: "assistant", Content: "another small value",
	}}))
	destination := filepath.Join(t.TempDir(), "content.db")
	_, err := database.BuildContentArchive(context.Background(), destination)
	require.NoError(t, err)

	archive, err := sql.Open(sqliteDriverName, makeDSN(destination, true))
	require.NoError(t, err)
	defer archive.Close()
	var packs, chunkRows, mappedRows int
	require.NoError(t, archive.QueryRow("SELECT count(*) FROM content_packs").Scan(&packs))
	require.NoError(t, archive.QueryRow("SELECT count(*) FROM content_chunks").Scan(&chunkRows))
	require.NoError(t, archive.QueryRow("SELECT count(*) FROM content_object_chunks").Scan(&mappedRows))
	assert.Equal(t, 1, packs)
	assert.Zero(t, chunkRows)
	assert.Zero(t, mappedRows)
	var pageSize int
	require.NoError(t, archive.QueryRow("PRAGMA page_size").Scan(&pageSize))
	assert.Equal(t, contentArchivePageSize, pageSize)
	var reverseIndexes int
	require.NoError(t, archive.QueryRow(`SELECT count(*) FROM sqlite_master
		WHERE type = 'index' AND tbl_name = 'content_refs'`).Scan(&reverseIndexes))
	assert.Zero(t, reverseIndexes)
	var rawObjects int
	require.NoError(t, archive.QueryRow(
		"SELECT count(*) FROM content_objects WHERE codec = ?", contentCodecRaw,
	).Scan(&rawObjects))
	assert.Positive(t, rawObjects)

	var entity, role, field int
	require.NoError(t, archive.QueryRow(`SELECT entity_kind, role, field_kind
		FROM content_refs WHERE message_ordinal = 0`).Scan(&entity, &role, &field))
	assert.Equal(t, entityKindCode["message"], entity)
	assert.Equal(t, roleCode["user"], role)
	assert.Equal(t, fieldKindCode["message.content"], field)
}

func TestContentArchiveReconstructsMultiChunkObject(t *testing.T) {
	database := testDB(t)
	insertSession(t, database, "session-a", "project")
	content := strings.Repeat("abcdefghij", contentArchiveChunkSize/5+17)
	require.Greater(t, len(content), contentArchiveChunkSize*2)
	require.NoError(t, database.InsertMessages([]Message{{
		SessionID: "session-a", Ordinal: 0, Role: "assistant", Content: content,
	}}))
	destination := filepath.Join(t.TempDir(), "content.db")
	report, err := database.BuildContentArchive(context.Background(), destination)
	require.NoError(t, err)
	require.NoError(t, VerifyContentArchive(context.Background(), destination))
	assert.Greater(t, report.UniqueChunks, int64(1))

	archive, err := sql.Open(sqliteDriverName, makeDSN(destination, true))
	require.NoError(t, err)
	defer archive.Close()
	var chunks, mappings int
	require.NoError(t, archive.QueryRow("SELECT count(*) FROM content_chunks").Scan(&chunks))
	require.NoError(t, archive.QueryRow("SELECT count(*) FROM content_object_chunks").Scan(&mappings))
	assert.Positive(t, chunks)
	assert.Greater(t, mappings, chunks-1)
}
