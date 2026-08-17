package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	_, err := database.getWriter().Exec(
		"UPDATE messages SET content = 'stale', thinking_text = 'stale'",
	)
	require.NoError(t, err)
	_, err = database.getWriter().Exec(
		"UPDATE tool_calls SET input_json = 'stale', result_content = 'stale'",
	)
	require.NoError(t, err)
	_, err = database.getWriter().Exec(
		"UPDATE tool_result_events SET content = 'stale'",
	)
	require.NoError(t, err)

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
