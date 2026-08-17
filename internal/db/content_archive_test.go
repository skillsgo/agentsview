package db

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
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
		"SELECT count(*) FROM content_refs WHERE role = 'user'",
	).Scan(&userRefs))
	require.NoError(t, archive.QueryRow(
		"SELECT count(*) FROM content_refs WHERE role = 'assistant'",
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
	_, err = archive.Exec("UPDATE content_chunks SET data = x'00'")
	require.NoError(t, err)
	require.NoError(t, archive.Close())

	err = VerifyContentArchive(context.Background(), destination)
	require.ErrorContains(t, err, "decompressing content chunk")
}
