package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"
)

// ErrArtifactExportLimit identifies a deterministic session-shape violation
// encountered before the complete export object graph is materialized.
var ErrArtifactExportLimit = errors.New("artifact export load limit exceeded")

// ArtifactExportLoadLimits bounds the source rows and raw string bytes that
// may be materialized for one artifact export.
type ArtifactExportLoadLimits struct {
	Messages            int
	UsageEvents         int
	MessageToolCalls    int
	ToolResultEvents    int
	SessionToolCalls    int
	SessionResultEvents int
	MessageBytes        int64
	UsageBytes          int64
}

// ArtifactExportData is one session's export-visible transcript and usage data
// loaded from a single SQLite read snapshot.
type ArtifactExportData struct {
	Messages    []Message
	UsageEvents []UsageEvent
}

const artifactMessageRawBytesSQL = `
	length(CAST(role AS BLOB)) +
	COALESCE((SELECT raw_size FROM content_objects WHERE id = content_object_id), 0) +
	COALESCE((SELECT raw_size FROM content_objects WHERE id = thinking_object_id), 0) +
	length(CAST(COALESCE(timestamp, '') AS BLOB)) +
	length(CAST(model AS BLOB)) +
	length(CAST(token_usage AS BLOB)) +
	length(CAST(claude_message_id AS BLOB)) +
	length(CAST(claude_request_id AS BLOB)) +
	length(CAST(source_type AS BLOB)) +
	length(CAST(source_subtype AS BLOB)) +
	length(CAST(source_uuid AS BLOB)) +
	length(CAST(source_parent_uuid AS BLOB)) +
	length(CAST(prompt_source AS BLOB))`

const artifactToolCallRawBytesSQL = `
	length(CAST(tool_name AS BLOB)) +
	length(CAST(category AS BLOB)) +
	length(CAST(COALESCE(tool_use_id, '') AS BLOB)) +
	COALESCE((SELECT raw_size FROM content_objects WHERE id = input_object_id), 0) +
	length(CAST(COALESCE(skill_name, '') AS BLOB)) +
	COALESCE((SELECT raw_size FROM content_objects WHERE id = result_object_id), 0) +
	length(CAST(COALESCE(subagent_session_id, '') AS BLOB)) +
	length(CAST(COALESCE(file_path, '') AS BLOB))`

const artifactResultEventRawBytesSQL = `
	length(CAST(COALESCE(tool_use_id, '') AS BLOB)) +
	length(CAST(COALESCE(agent_id, '') AS BLOB)) +
	length(CAST(COALESCE(subagent_session_id, '') AS BLOB)) +
	length(CAST(source AS BLOB)) +
	length(CAST(status AS BLOB)) +
	COALESCE((SELECT raw_size FROM content_objects WHERE id = content_object_id), 0) +
	length(CAST(COALESCE(timestamp, '') AS BLOB))`

const artifactUsageRawBytesSQL = `
	length(CAST(source AS BLOB)) +
	length(CAST(model AS BLOB)) +
	length(CAST(cost_status AS BLOB)) +
	length(CAST(cost_source AS BLOB)) +
	length(CAST(COALESCE(occurred_at, '') AS BLOB)) +
	length(CAST(dedup_key AS BLOB))`

// LoadArtifactExportData preflights and materializes one session from the same
// read transaction so concurrent writes cannot invalidate the bounds.
func (db *DB) LoadArtifactExportData(
	ctx context.Context, sessionID string, limits ArtifactExportLoadLimits,
) (_ ArtifactExportData, retErr error) {
	if sessionID == "" {
		return ArtifactExportData{}, errors.New("artifact export session id is required")
	}
	if err := validateArtifactExportLoadLimits(limits); err != nil {
		return ArtifactExportData{}, err
	}
	db.connMu.RLock()
	reader := db.reader.Load()
	if reader == nil {
		db.connMu.RUnlock()
		return ArtifactExportData{}, errors.New("database is closed")
	}
	tx, err := reader.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	db.connMu.RUnlock()
	if err != nil {
		return ArtifactExportData{}, fmt.Errorf("beginning artifact export snapshot: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			retErr = errors.Join(retErr, tx.Rollback())
		}
	}()

	if err := preflightArtifactExportTx(ctx, tx, sessionID, limits); err != nil {
		return ArtifactExportData{}, err
	}
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`
		SELECT %s FROM messages
		WHERE session_id = ?
		ORDER BY ordinal ASC
		LIMIT ?`, selectMessageCols), sessionID, limits.Messages+1)
	if err != nil {
		return ArtifactExportData{}, fmt.Errorf("loading artifact export messages: %w", err)
	}
	messages, scanErr := scanMessages(rows)
	closeErr := rows.Close()
	if scanErr != nil || closeErr != nil {
		return ArtifactExportData{}, errors.Join(scanErr, closeErr)
	}
	if len(messages) > limits.Messages {
		return ArtifactExportData{}, artifactExportLimitf(
			"session %s message count exceeds %d", sessionID, limits.Messages,
		)
	}
	if err := hydrateMessageContents(ctx, tx, messages); err != nil {
		return ArtifactExportData{}, err
	}
	if err := attachArtifactNestedCollectionsTx(
		ctx, tx, sessionID, messages, limits,
	); err != nil {
		return ArtifactExportData{}, err
	}
	usage, err := usageEventsWithQuerier(
		ctx, tx, sessionID, limits.UsageEvents+1,
	)
	if err != nil {
		return ArtifactExportData{}, err
	}
	if len(usage) > limits.UsageEvents {
		return ArtifactExportData{}, artifactExportLimitf(
			"session %s usage count exceeds %d", sessionID, limits.UsageEvents,
		)
	}
	if err := tx.Commit(); err != nil {
		return ArtifactExportData{}, fmt.Errorf("committing artifact export snapshot: %w", err)
	}
	committed = true
	return ArtifactExportData{Messages: messages, UsageEvents: usage}, nil
}

func attachArtifactNestedCollectionsTx(
	ctx context.Context,
	tx *sql.Tx,
	sessionID string,
	messages []Message,
	limits ArtifactExportLoadLimits,
) error {
	if len(messages) == 0 {
		return nil
	}
	messageByID := make(map[int64]int, len(messages))
	messageByOrdinal := make(map[int]int, len(messages))
	for i, message := range messages {
		messageByID[message.ID] = i
		messageByOrdinal[message.Ordinal] = i
	}

	type toolCallRow struct {
		rowID int64
		call  ToolCall
	}
	rows, err := tx.QueryContext(ctx, `
		SELECT id, message_id, session_id, tool_name, category,
			tool_use_id, input_object_id, skill_name,
			result_content_length, result_object_id,
			subagent_session_id,
			file_path, call_index
		FROM tool_calls
		WHERE session_id = ?
		LIMIT ?`, sessionID, limits.SessionToolCalls+1)
	if err != nil {
		return fmt.Errorf("loading bounded artifact tool calls: %w", err)
	}
	toolCalls := make([]toolCallRow, 0)
	for rows.Next() {
		var row toolCallRow
		var toolUseID, skillName sql.NullString
		var inputObjectID, resultObjectID sql.NullInt64
		var subagentSessionID sql.NullString
		var filePath sql.NullString
		var resultLen sql.NullInt64
		var callIndex sql.NullInt64
		if err := rows.Scan(
			&row.rowID, &row.call.MessageID, &row.call.SessionID,
			&row.call.ToolName, &row.call.Category,
			&toolUseID, &inputObjectID, &skillName,
			&resultLen, &resultObjectID,
			&subagentSessionID,
			&filePath, &callIndex,
		); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scanning bounded artifact tool call: %w", err)
		}
		if toolUseID.Valid {
			row.call.ToolUseID = toolUseID.String
		}
		if inputObjectID.Valid {
			row.call.inputObjectID = &inputObjectID.Int64
		}
		if skillName.Valid {
			row.call.SkillName = skillName.String
		}
		if resultLen.Valid {
			row.call.ResultContentLength = int(resultLen.Int64)
		}
		if resultObjectID.Valid {
			row.call.resultObjectID = &resultObjectID.Int64
		}
		if subagentSessionID.Valid {
			row.call.SubagentSessionID = subagentSessionID.String
		}
		if filePath.Valid {
			row.call.FilePath = filePath.String
		}
		if callIndex.Valid {
			row.call.CallIndex = int(callIndex.Int64)
		}
		toolCalls = append(toolCalls, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterating bounded artifact tool calls: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("closing bounded artifact tool calls: %w", err)
	}
	if len(toolCalls) > limits.SessionToolCalls {
		return artifactExportLimitf(
			"session %s tool call count exceeds %d",
			sessionID, limits.SessionToolCalls,
		)
	}
	toolContentIDs := make([]int64, 0, len(toolCalls)*2)
	for i := range toolCalls {
		if toolCalls[i].call.inputObjectID != nil {
			toolContentIDs = append(toolContentIDs, *toolCalls[i].call.inputObjectID)
		}
		if toolCalls[i].call.resultObjectID != nil {
			toolContentIDs = append(toolContentIDs, *toolCalls[i].call.resultObjectID)
		}
	}
	toolContents, err := loadAgentContents(ctx, tx, toolContentIDs)
	if err != nil {
		return err
	}
	for i := range toolCalls {
		call := &toolCalls[i].call
		if call.inputObjectID != nil {
			call.InputJSON = toolContents[*call.inputObjectID]
		}
		if call.resultObjectID != nil {
			call.ResultContent = toolContents[*call.resultObjectID]
		}
		call.inputObjectID, call.resultObjectID = nil, nil
	}
	sort.Slice(toolCalls, func(i, j int) bool {
		if toolCalls[i].call.MessageID != toolCalls[j].call.MessageID {
			return toolCalls[i].call.MessageID < toolCalls[j].call.MessageID
		}
		if toolCalls[i].call.CallIndex != toolCalls[j].call.CallIndex {
			return toolCalls[i].call.CallIndex < toolCalls[j].call.CallIndex
		}
		return toolCalls[i].rowID < toolCalls[j].rowID
	})
	for _, row := range toolCalls {
		if messageIndex, ok := messageByID[row.call.MessageID]; ok {
			messages[messageIndex].ToolCalls = append(
				messages[messageIndex].ToolCalls, row.call,
			)
		}
	}

	type resultEventRow struct {
		rowID          int64
		messageOrdinal int
		callIndex      int
		event          ToolResultEvent
	}
	rows, err = tx.QueryContext(ctx, `
		SELECT id, tool_call_message_ordinal, call_index,
			tool_use_id, agent_id, subagent_session_id,
			source, status, content_object_id, content_length,
			timestamp, event_index
		FROM tool_result_events
		WHERE session_id = ?
		LIMIT ?`, sessionID, limits.SessionResultEvents+1)
	if err != nil {
		return fmt.Errorf("loading bounded artifact result events: %w", err)
	}
	resultEvents := make([]resultEventRow, 0)
	for rows.Next() {
		var row resultEventRow
		var toolUseID, agentID, subagentSessionID, timestamp sql.NullString
		if err := rows.Scan(
			&row.rowID, &row.messageOrdinal, &row.callIndex,
			&toolUseID, &agentID, &subagentSessionID,
			&row.event.Source, &row.event.Status, &row.event.contentObjectID,
			&row.event.ContentLength, &timestamp, &row.event.EventIndex,
		); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scanning bounded artifact result event: %w", err)
		}
		if toolUseID.Valid {
			row.event.ToolUseID = toolUseID.String
		}
		if agentID.Valid {
			row.event.AgentID = agentID.String
		}
		if subagentSessionID.Valid {
			row.event.SubagentSessionID = subagentSessionID.String
		}
		if timestamp.Valid {
			row.event.Timestamp = timestamp.String
		}
		resultEvents = append(resultEvents, row)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterating bounded artifact result events: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("closing bounded artifact result events: %w", err)
	}
	if len(resultEvents) > limits.SessionResultEvents {
		return artifactExportLimitf(
			"session %s result event count exceeds %d",
			sessionID, limits.SessionResultEvents,
		)
	}
	eventContentIDs := make([]int64, 0, len(resultEvents))
	for i := range resultEvents {
		if resultEvents[i].event.contentObjectID != nil {
			eventContentIDs = append(eventContentIDs,
				*resultEvents[i].event.contentObjectID)
		}
	}
	eventContents, err := loadAgentContents(ctx, tx, eventContentIDs)
	if err != nil {
		return err
	}
	for i := range resultEvents {
		event := &resultEvents[i].event
		if event.contentObjectID != nil {
			event.Content = eventContents[*event.contentObjectID]
		}
		event.contentObjectID = nil
	}
	sort.Slice(resultEvents, func(i, j int) bool {
		if resultEvents[i].messageOrdinal != resultEvents[j].messageOrdinal {
			return resultEvents[i].messageOrdinal < resultEvents[j].messageOrdinal
		}
		if resultEvents[i].callIndex != resultEvents[j].callIndex {
			return resultEvents[i].callIndex < resultEvents[j].callIndex
		}
		if resultEvents[i].event.EventIndex != resultEvents[j].event.EventIndex {
			return resultEvents[i].event.EventIndex < resultEvents[j].event.EventIndex
		}
		return resultEvents[i].rowID < resultEvents[j].rowID
	})
	for _, row := range resultEvents {
		messageIndex, ok := messageByOrdinal[row.messageOrdinal]
		if !ok || row.callIndex < 0 ||
			row.callIndex >= len(messages[messageIndex].ToolCalls) {
			continue
		}
		messages[messageIndex].ToolCalls[row.callIndex].ResultEvents = append(
			messages[messageIndex].ToolCalls[row.callIndex].ResultEvents,
			row.event,
		)
	}
	return nil
}

func validateArtifactExportLoadLimits(limits ArtifactExportLoadLimits) error {
	if limits.Messages < 1 || limits.UsageEvents < 1 ||
		limits.MessageToolCalls < 1 || limits.ToolResultEvents < 1 ||
		limits.SessionToolCalls < 1 || limits.SessionResultEvents < 1 ||
		limits.MessageBytes < 1 || limits.UsageBytes < 1 {
		return errors.New("artifact export load limits must all be positive")
	}
	if limits.Messages == math.MaxInt || limits.UsageEvents == math.MaxInt ||
		limits.SessionToolCalls == math.MaxInt ||
		limits.SessionResultEvents == math.MaxInt {
		return errors.New("artifact export load limits must permit a limit-plus-one probe")
	}
	return nil
}

func preflightArtifactExportTx(
	ctx context.Context, tx *sql.Tx, sessionID string, limits ArtifactExportLoadLimits,
) error {
	messageBytes, err := preflightArtifactMessagesTx(ctx, tx, sessionID, limits)
	if err != nil {
		return err
	}
	nestedBytes, err := preflightArtifactToolCallsTx(ctx, tx, sessionID, limits)
	if err != nil {
		return err
	}
	resultBytes, err := preflightArtifactResultEventsTx(ctx, tx, sessionID, limits)
	if err != nil {
		return err
	}
	totalBytes, exceeded := addArtifactExportBytes(0, messageBytes, limits.MessageBytes)
	if !exceeded {
		totalBytes, exceeded = addArtifactExportBytes(
			totalBytes, nestedBytes, limits.MessageBytes,
		)
	}
	if !exceeded {
		_, exceeded = addArtifactExportBytes(
			totalBytes, resultBytes, limits.MessageBytes,
		)
	}
	if exceeded {
		return artifactExportLimitf(
			"session %s raw message bytes exceed %d", sessionID, limits.MessageBytes,
		)
	}
	return preflightArtifactUsageTx(ctx, tx, sessionID, limits)
}

func preflightArtifactMessagesTx(
	ctx context.Context, tx *sql.Tx, sessionID string, limits ArtifactExportLoadLimits,
) (int64, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, ordinal, `+artifactMessageRawBytesSQL+`
		FROM messages WHERE session_id = ? LIMIT ?`,
		sessionID, limits.Messages+1,
	)
	if err != nil {
		return 0, fmt.Errorf("preflighting artifact messages: %w", err)
	}
	var count int
	var totalBytes int64
	for rows.Next() {
		var id int64
		var ordinal int
		var rawBytes int64
		if err := rows.Scan(&id, &ordinal, &rawBytes); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scanning artifact message preflight: %w", err)
		}
		count++
		if count > limits.Messages {
			return 0, closeArtifactPreflightAtLimit(rows, artifactExportLimitf(
				"session %s message count exceeds %d", sessionID, limits.Messages,
			))
		}
		var exceeded bool
		totalBytes, exceeded = addArtifactExportBytes(
			totalBytes, rawBytes, limits.MessageBytes,
		)
		if exceeded {
			return 0, closeArtifactPreflightAtLimit(rows, artifactExportLimitf(
				"session %s raw message bytes exceed %d",
				sessionID, limits.MessageBytes,
			))
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("iterating artifact message preflight: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("closing artifact message preflight: %w", err)
	}
	return totalBytes, nil
}

func preflightArtifactToolCallsTx(
	ctx context.Context, tx *sql.Tx, sessionID string, limits ArtifactExportLoadLimits,
) (int64, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT message_id, `+artifactToolCallRawBytesSQL+`
		FROM tool_calls WHERE session_id = ? LIMIT ?`,
		sessionID, limits.SessionToolCalls+1,
	)
	if err != nil {
		return 0, fmt.Errorf("preflighting artifact tool calls: %w", err)
	}
	perMessage := make(map[int64]int)
	var count int
	var totalBytes int64
	for rows.Next() {
		var messageID int64
		var rawBytes int64
		if err := rows.Scan(&messageID, &rawBytes); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scanning artifact tool call preflight: %w", err)
		}
		count++
		perMessage[messageID]++
		if count > limits.SessionToolCalls {
			return 0, closeArtifactPreflightAtLimit(rows, artifactExportLimitf(
				"session %s tool call count exceeds %d",
				sessionID, limits.SessionToolCalls,
			))
		}
		if perMessage[messageID] > limits.MessageToolCalls {
			return 0, closeArtifactPreflightAtLimit(rows, artifactExportLimitf(
				"session %s message tool call count exceeds %d",
				sessionID, limits.MessageToolCalls,
			))
		}
		var exceeded bool
		totalBytes, exceeded = addArtifactExportBytes(
			totalBytes, rawBytes, limits.MessageBytes,
		)
		if exceeded {
			return 0, closeArtifactPreflightAtLimit(rows, artifactExportLimitf(
				"session %s raw message bytes exceed %d",
				sessionID, limits.MessageBytes,
			))
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("iterating artifact tool call preflight: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("closing artifact tool call preflight: %w", err)
	}
	return totalBytes, nil
}

func preflightArtifactResultEventsTx(
	ctx context.Context, tx *sql.Tx, sessionID string, limits ArtifactExportLoadLimits,
) (int64, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT tool_call_message_ordinal, call_index,
		       `+artifactResultEventRawBytesSQL+`
		FROM tool_result_events WHERE session_id = ? LIMIT ?`,
		sessionID, limits.SessionResultEvents+1,
	)
	if err != nil {
		return 0, fmt.Errorf("preflighting artifact result events: %w", err)
	}
	type callKey struct {
		messageOrdinal int
		callIndex      int
	}
	perCall := make(map[callKey]int)
	var count int
	var totalBytes int64
	for rows.Next() {
		var key callKey
		var rawBytes int64
		if err := rows.Scan(&key.messageOrdinal, &key.callIndex, &rawBytes); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scanning artifact result event preflight: %w", err)
		}
		count++
		perCall[key]++
		if count > limits.SessionResultEvents {
			return 0, closeArtifactPreflightAtLimit(rows, artifactExportLimitf(
				"session %s result event count exceeds %d",
				sessionID, limits.SessionResultEvents,
			))
		}
		if perCall[key] > limits.ToolResultEvents {
			return 0, closeArtifactPreflightAtLimit(rows, artifactExportLimitf(
				"session %s tool result event count exceeds %d",
				sessionID, limits.ToolResultEvents,
			))
		}
		var exceeded bool
		totalBytes, exceeded = addArtifactExportBytes(
			totalBytes, rawBytes, limits.MessageBytes,
		)
		if exceeded {
			return 0, closeArtifactPreflightAtLimit(rows, artifactExportLimitf(
				"session %s raw message bytes exceed %d",
				sessionID, limits.MessageBytes,
			))
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("iterating artifact result event preflight: %w", err)
	}
	if err := rows.Close(); err != nil {
		return 0, fmt.Errorf("closing artifact result event preflight: %w", err)
	}
	return totalBytes, nil
}

func preflightArtifactUsageTx(
	ctx context.Context, tx *sql.Tx, sessionID string, limits ArtifactExportLoadLimits,
) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT `+artifactUsageRawBytesSQL+`
		FROM usage_events WHERE session_id = ? LIMIT ?`,
		sessionID, limits.UsageEvents+1,
	)
	if err != nil {
		return fmt.Errorf("preflighting artifact usage events: %w", err)
	}
	var count int
	var totalBytes int64
	for rows.Next() {
		var rawBytes int64
		if err := rows.Scan(&rawBytes); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scanning artifact usage preflight: %w", err)
		}
		count++
		if count > limits.UsageEvents {
			return closeArtifactPreflightAtLimit(rows, artifactExportLimitf(
				"session %s usage event count exceeds %d",
				sessionID, limits.UsageEvents,
			))
		}
		var exceeded bool
		totalBytes, exceeded = addArtifactExportBytes(
			totalBytes, rawBytes, limits.UsageBytes,
		)
		if exceeded {
			return closeArtifactPreflightAtLimit(rows, artifactExportLimitf(
				"session %s raw usage bytes exceed %d",
				sessionID, limits.UsageBytes,
			))
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("iterating artifact usage preflight: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("closing artifact usage preflight: %w", err)
	}
	return nil
}

func closeArtifactPreflightAtLimit(rows *sql.Rows, limitErr error) error {
	if err := rows.Close(); err != nil {
		return fmt.Errorf("closing artifact preflight at limit: %w", err)
	}
	return limitErr
}

func addArtifactExportBytes(current, additional, limit int64) (int64, bool) {
	if current > limit || additional < 0 || additional > limit-current {
		return current, true
	}
	return current + additional, false
}

func artifactExportLimitf(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrArtifactExportLimit, fmt.Sprintf(format, args...))
}
