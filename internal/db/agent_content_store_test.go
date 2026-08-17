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
