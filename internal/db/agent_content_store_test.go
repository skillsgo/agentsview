package db

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAgentContentStoreSchemaHasOnlyObjectReferences(t *testing.T) {
	database := testDB(t)

	tests := []struct {
		table      string
		want       []string
		deprecated []string
	}{
		{
			table:      "messages",
			want:       []string{"content_object_id", "thinking_object_id"},
			deprecated: []string{"content", "thinking_text"},
		},
		{
			table:      "tool_calls",
			want:       []string{"input_object_id", "result_object_id"},
			deprecated: []string{"input_json", "result_content"},
		},
		{
			table:      "tool_result_events",
			want:       []string{"content_object_id"},
			deprecated: []string{"content"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.table, func(t *testing.T) {
			columns := tableColumnNames(t, database.getReader(), tc.table)
			for _, column := range tc.want {
				assert.Contains(t, columns, column)
			}
			for _, column := range tc.deprecated {
				assert.NotContains(t, columns, column)
			}
		})
	}
}

type schemaQueryer interface {
	Query(string, ...any) (*sql.Rows, error)
}

func tableColumnNames(t *testing.T, database schemaQueryer, table string) []string {
	t.Helper()
	rows, err := database.Query("SELECT name FROM pragma_table_info(?)", table)
	require.NoError(t, err)
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var column string
		require.NoError(t, rows.Scan(&column))
		columns = append(columns, column)
	}
	require.NoError(t, rows.Err())
	return columns
}

func TestAgentContentStoreDualWritesExactSharedBodies(t *testing.T) {
	database := testDB(t)
	insertSession(t, database, "agent-store", "project")
	sharedMessage := "same prompt"
	sharedResult := "same tool result"
	require.NoError(t, database.InsertMessages([]Message{{
		SessionID: "agent-store", Ordinal: 0, Role: "user", Content: sharedMessage,
	}, {
		SessionID: "agent-store", Ordinal: 1, Role: "assistant", Content: sharedMessage,
		ThinkingText: "private reasoning", HasToolUse: true,
		ToolCalls: []ToolCall{{
			ToolName: "exec", InputJSON: `{"cmd":"true"}`,
			ResultContent: sharedResult,
			ResultEvents: []ToolResultEvent{{
				Source: "tool_result", Status: "completed", Content: sharedResult,
			}},
		}},
	}}))

	var objects int
	require.NoError(t, database.getReader().QueryRow(
		"SELECT count(*) FROM content_objects",
	).Scan(&objects))
	assert.Equal(t, 4, objects)
	if database.HasFTS() {
		var projected int
		require.NoError(t, database.getReader().QueryRow(
			"SELECT count(*) FROM content_fts",
		).Scan(&projected))
		assert.Equal(t, objects, projected,
			"each unique object should have one search projection")
	}

	var firstMessageID, secondMessageID int64
	require.NoError(t, database.getReader().QueryRow(`SELECT
		(SELECT content_object_id FROM messages WHERE ordinal = 0),
		(SELECT content_object_id FROM messages WHERE ordinal = 1)`).Scan(
		&firstMessageID, &secondMessageID,
	))
	assert.Equal(t, firstMessageID, secondMessageID)

	var resultID, eventID int64
	require.NoError(t, database.getReader().QueryRow(`SELECT
		(SELECT result_object_id FROM tool_calls LIMIT 1),
		(SELECT content_object_id FROM tool_result_events LIMIT 1)`).Scan(
		&resultID, &eventID,
	))
	assert.Equal(t, resultID, eventID)

	content, err := readAgentContent(context.Background(),
		func(query string, args ...any) rowScanner {
			return database.getReader().QueryRowContext(context.Background(), query, args...)
		}, resultID)
	require.NoError(t, err)
	assert.Equal(t, sharedResult, content)
}

func TestAgentContentStoreSkipsEmptyBodies(t *testing.T) {
	database := testDB(t)
	insertSession(t, database, "empty-content", "project")
	require.NoError(t, database.InsertMessages([]Message{{
		SessionID: "empty-content", Ordinal: 0, Role: "assistant", Content: "visible",
		ToolCalls: []ToolCall{{ToolName: "noop"}},
	}}))
	var thinkingID, inputID, resultID *int64
	require.NoError(t, database.getReader().QueryRow(`SELECT
		m.thinking_object_id, tc.input_object_id, tc.result_object_id
		FROM messages m JOIN tool_calls tc ON tc.message_id = m.id`).Scan(
		&thinkingID, &inputID, &resultID,
	))
	assert.Nil(t, thinkingID)
	assert.Nil(t, inputID)
	assert.Nil(t, resultID)
}

func TestAgentContentStoreReclaimsOnlyUnreferencedBodies(t *testing.T) {
	database := testDB(t)
	insertSession(t, database, "content-owner-a", "project")
	insertSession(t, database, "content-owner-b", "project")
	for _, sessionID := range []string{"content-owner-a", "content-owner-b"} {
		require.NoError(t, database.InsertMessages([]Message{{
			SessionID: sessionID, Ordinal: 0, Role: "user", Content: "shared body",
		}}))
	}

	var objectID int64
	var refs int
	require.NoError(t, database.getReader().QueryRow(
		"SELECT id, ref_count FROM content_objects",
	).Scan(&objectID, &refs))
	assert.Equal(t, 2, refs)
	require.NoError(t, database.ReplaceSessionMessages("content-owner-a", []Message{{
		SessionID: "content-owner-a", Ordinal: 0, Role: "user",
		Content: "shared body", Timestamp: "2026-08-17T00:00:00Z",
	}}))
	require.NoError(t, database.getReader().QueryRow(
		"SELECT ref_count FROM content_objects WHERE id = ?", objectID,
	).Scan(&refs))
	assert.Equal(t, 2, refs, "in-place updates must not leak reservations")

	require.NoError(t, database.DeleteSession("content-owner-a"))
	require.NoError(t, database.getReader().QueryRow(
		"SELECT ref_count FROM content_objects WHERE id = ?", objectID,
	).Scan(&refs))
	assert.Equal(t, 1, refs)

	require.NoError(t, database.DeleteSession("content-owner-b"))
	var objects int
	require.NoError(t, database.getReader().QueryRow(
		"SELECT count(*) FROM content_objects WHERE id = ?", objectID,
	).Scan(&objects))
	assert.Zero(t, objects)
	if database.hasContentFTS() {
		var projected int
		require.NoError(t, database.getReader().QueryRow(
			"SELECT count(*) FROM content_fts WHERE rowid = ?", objectID,
		).Scan(&projected))
		assert.Zero(t, projected)
	}
}

func TestAgentContentStoreHydratesFromAuthoritativeObjects(t *testing.T) {
	database := testDB(t)
	insertSession(t, database, "hydrate-content", "project")
	require.NoError(t, database.InsertMessages([]Message{{
		SessionID: "hydrate-content", Ordinal: 0, Role: "assistant",
		Content: "authoritative message", ThinkingText: "authoritative thought",
		ToolCalls: []ToolCall{{
			ToolName: "exec", InputJSON: `{"cmd":"true"}`,
			ResultContent: "authoritative result",
			ResultEvents: []ToolResultEvent{{
				Source: "tool_result", Status: "completed",
				Content: "authoritative event",
			}},
		}},
	}}))

	messages, err := database.GetAllMessages(context.Background(), "hydrate-content")
	require.NoError(t, err)
	require.Len(t, messages, 1)
	assert.Equal(t, "authoritative message", messages[0].Content)
	assert.Equal(t, "authoritative thought", messages[0].ThinkingText)
	require.Len(t, messages[0].ToolCalls, 1)
	assert.Equal(t, `{"cmd":"true"}`, messages[0].ToolCalls[0].InputJSON)
	assert.Equal(t, "authoritative result", messages[0].ToolCalls[0].ResultContent)
	require.Len(t, messages[0].ToolCalls[0].ResultEvents, 1)
	assert.Equal(t, "authoritative event",
		messages[0].ToolCalls[0].ResultEvents[0].Content)
	if database.HasFTS() {
		var units []EmbeddableUnit
		_, err := database.ScanEmbeddableUnits(
			context.Background(), "", true,
			func(unit EmbeddableUnit) error {
				units = append(units, unit)
				return nil
			},
		)
		require.NoError(t, err)
		require.Len(t, units, 1)
		assert.Equal(t, "authoritative message", units[0].Content)
	}
}
