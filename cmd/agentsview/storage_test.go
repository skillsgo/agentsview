package main

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/skillsgo/agentsview/internal/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageCompareBuildsAndVerifiesArchive(t *testing.T) {
	dataDir := t.TempDir()
	t.Setenv("AGENTSVIEW_DATA_DIR", dataDir)
	source, err := db.Open(filepath.Join(dataDir, "sessions.db"))
	require.NoError(t, err)
	require.NoError(t, source.UpsertSession(db.Session{
		ID: "session-a", Project: "project", Machine: "local", Agent: "codex",
	}))
	require.NoError(t, source.InsertMessages([]db.Message{{
		SessionID: "session-a",
		Ordinal:   0,
		Role:      "user",
		Content:   "repeated prompt",
	}, {
		SessionID: "session-a",
		Ordinal:   1,
		Role:      "assistant",
		Content:   "repeated prompt",
	}}))
	require.NoError(t, source.Close())

	destination := filepath.Join(t.TempDir(), "content.db")
	output, err := executeCommand(
		newRootCommand(), "storage", "compare",
		"--archive", destination, "--format", "json",
	)
	require.NoError(t, err)
	var report db.ContentArchiveReport
	require.NoError(t, json.Unmarshal([]byte(output), &report))
	assert.Equal(t, int64(2), report.References)
	assert.Equal(t, int64(1), report.UniqueObjects)
	assert.Equal(t, int64(len("repeated prompt")), report.DuplicateBytesEliminated)
	require.NoError(t, db.VerifyContentArchive(t.Context(), destination))
}

func TestStorageCompareRequiresDestination(t *testing.T) {
	_, err := executeCommand(newRootCommand(), "storage", "compare")
	require.ErrorContains(t, err, "--archive is required")
}
