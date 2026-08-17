package db

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"strings"

	"github.com/klauspost/compress/zstd"
)

func auditAutomatedFull(
	w *writerHandle,
	patterns automationPatternSnapshot,
) (setIDs, clearIDs []string, err error) {
	return auditAutomatedFromAgentContent(w, patterns)
}

// Matching-hash audits used to inspect an inline byte prefix and fetch the
// full text only for ambiguous rows. Compressed objects cannot expose a prefix
// without decoding, so the authoritative-object pass is simpler and exact. It
// reads only one user body per session and deduplicates shared object decoding.
func auditAutomatedMatchingHash(
	w *writerHandle,
	patterns automationPatternSnapshot,
) (setIDs, clearIDs []string, err error) {
	type boundedCandidate struct {
		id               string
		userMessageCount int
		rowAutomated     bool
		firstUserID      sql.NullInt64
		firstMessage     AutomationTextEvidence
	}
	rows, err := w.Query(`SELECT s.id, s.user_message_count, s.is_automated,
		(SELECT m.content_object_id FROM messages m
		 WHERE m.session_id = s.id AND m.role = 'user' AND m.is_system = 0
		   AND m.content_object_id IS NOT NULL ORDER BY m.ordinal LIMIT 1),
		substr(CAST(s.first_message AS BLOB), 1, ?),
		octet_length(s.first_message)
		FROM sessions s`, AutomationEvidencePrefixBytes)
	if err != nil {
		return nil, nil, err
	}
	var candidates []boundedCandidate
	var objectIDs []int64
	for rows.Next() {
		var candidate boundedCandidate
		var prefix []byte
		var length sql.NullInt64
		if err := rows.Scan(&candidate.id, &candidate.userMessageCount,
			&candidate.rowAutomated, &candidate.firstUserID,
			&prefix, &length); err != nil {
			_ = rows.Close()
			return nil, nil, fmt.Errorf("scanning bounded automated audit candidate: %w", err)
		}
		candidate.firstMessage = AutomationTextEvidence{
			Prefix: prefix, FullByteLength: length.Int64, Valid: length.Valid,
		}
		candidates = append(candidates, candidate)
		if candidate.userMessageCount <= 1 && candidate.firstUserID.Valid {
			objectIDs = append(objectIDs, candidate.firstUserID.Int64)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}
	evidence, err := loadAgentContentEvidence(w, objectIDs)
	if err != nil {
		return nil, nil, err
	}
	var unresolvedIDs []string
	for _, candidate := range candidates {
		var firstUser AutomationTextEvidence
		if candidate.firstUserID.Valid {
			firstUser = evidence[candidate.firstUserID.Int64]
		}
		want, conclusive := patterns.verdictFromEvidence(
			candidate.userMessageCount, firstUser, candidate.firstMessage,
		)
		if !conclusive {
			unresolvedIDs = append(unresolvedIDs, candidate.id)
			continue
		}
		setIDs, clearIDs = appendAutomationFlagChange(
			setIDs, clearIDs, candidate.id, candidate.rowAutomated, want,
		)
	}
	unresolved, err := queryAutomationCandidates(w, unresolvedIDs)
	if err != nil {
		return nil, nil, err
	}
	batchSet, batchClear, err := classifyAutomationCandidates(w, patterns, unresolved)
	if err != nil {
		return nil, nil, err
	}
	return append(setIDs, batchSet...), append(clearIDs, batchClear...), nil
}

type automationCandidate struct {
	id               string
	firstMessage     sql.NullString
	userMessageCount int
	rowAutomated     bool
	firstUserID      sql.NullInt64
}

// auditAutomatedFromAgentContent classifies sessions from the authoritative
// compressed first-user body. first_message remains a deliberately small UI
// summary; full Agent bodies are never read from normalized inline columns.
func auditAutomatedFromAgentContent(
	w *writerHandle,
	patterns automationPatternSnapshot,
) (setIDs, clearIDs []string, err error) {
	candidates, err := queryAutomationCandidates(w, nil)
	if err != nil {
		return nil, nil, err
	}
	return classifyAutomationCandidates(w, patterns, candidates)
}

func queryAutomationCandidates(
	w *writerHandle, ids []string,
) ([]automationCandidate, error) {
	where := ""
	var args []any
	if ids != nil {
		if len(ids) == 0 {
			return nil, nil
		}
		where, args = inPlaceholders(ids)
		where = " WHERE s.id IN " + where
	}
	rows, err := w.Query(`SELECT
			s.id, s.first_message, s.user_message_count, s.is_automated,
			(SELECT m.content_object_id
			 FROM messages m
			 WHERE m.session_id = s.id
			   AND m.role = 'user'
			   AND m.is_system = 0
			   AND m.content_object_id IS NOT NULL
			 ORDER BY m.ordinal
			 LIMIT 1)
		FROM sessions s`+where, args...)
	if err != nil {
		return nil, fmt.Errorf(
			"querying automated backfill candidates: %w", err,
		)
	}
	var candidates []automationCandidate
	var objectIDs []int64
	for rows.Next() {
		var candidate automationCandidate
		if err := rows.Scan(
			&candidate.id, &candidate.firstMessage,
			&candidate.userMessageCount, &candidate.rowAutomated,
			&candidate.firstUserID,
		); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf(
				"scanning automated audit candidate: %w", err,
			)
		}
		candidates = append(candidates, candidate)
		if candidate.firstUserID.Valid {
			objectIDs = append(objectIDs, candidate.firstUserID.Int64)
		}
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return candidates, nil
}

func classifyAutomationCandidates(
	w *writerHandle,
	patterns automationPatternSnapshot,
	candidates []automationCandidate,
) (setIDs, clearIDs []string, err error) {
	var objectIDs []int64
	for _, candidate := range candidates {
		if candidate.firstUserID.Valid {
			objectIDs = append(objectIDs, candidate.firstUserID.Int64)
		}
	}
	contents, err := loadAgentContents(context.Background(), w, objectIDs)
	if err != nil {
		return nil, nil, err
	}
	for _, candidate := range candidates {
		var firstUser sql.NullString
		if candidate.firstUserID.Valid {
			firstUser = sql.NullString{
				String: contents[candidate.firstUserID.Int64], Valid: true,
			}
		}
		want := patterns.matchesTextCandidates(
			candidate.userMessageCount, firstUser, candidate.firstMessage,
		)
		setIDs, clearIDs = appendAutomationFlagChange(
			setIDs, clearIDs, candidate.id, candidate.rowAutomated, want,
		)
	}
	return setIDs, clearIDs, nil
}

func loadAgentContentEvidence(
	w *writerHandle, ids []int64,
) (map[int64]AutomationTextEvidence, error) {
	result := make(map[int64]AutomationTextEvidence, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	unique := make(map[int64]struct{}, len(ids))
	args := make([]any, 0, len(ids)+1)
	marks := make([]string, 0, len(ids))
	args = append(args, AutomationEvidencePrefixBytes)
	for _, id := range ids {
		if _, exists := unique[id]; exists {
			continue
		}
		unique[id] = struct{}{}
		marks = append(marks, "?")
		args = append(args, id)
	}
	rows, err := w.Query(`SELECT id, raw_size, codec,
		CASE WHEN codec = 0 THEN substr(payload, 1, ?) ELSE payload END
		FROM content_objects WHERE id IN (`+strings.Join(marks, ",")+`)`, args...)
	if err != nil {
		return nil, fmt.Errorf("querying Agent content evidence: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var rawSize, codec int
		var payload []byte
		if err := rows.Scan(&id, &rawSize, &codec, &payload); err != nil {
			return nil, fmt.Errorf("scanning Agent content evidence: %w", err)
		}
		prefix := payload
		if codec == contentCodecZstd {
			decoder, err := zstd.NewReader(bytes.NewReader(payload), zstd.WithDecoderConcurrency(1))
			if err != nil {
				return nil, fmt.Errorf("opening Agent content evidence %d: %w", id, err)
			}
			prefix, err = io.ReadAll(io.LimitReader(decoder, AutomationEvidencePrefixBytes))
			decoder.Close()
			if err != nil {
				return nil, fmt.Errorf("decoding Agent content evidence %d: %w", id, err)
			}
		} else if codec != contentCodecRaw {
			return nil, fmt.Errorf("unknown Agent content codec %d", codec)
		}
		result[id] = AutomationTextEvidence{
			Prefix: prefix, FullByteLength: int64(rawSize), Valid: true,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(result) != len(unique) {
		return nil, fmt.Errorf("Agent content evidence reference is missing")
	}
	return result, nil
}

func appendAutomationFlagChange(
	setIDs, clearIDs []string,
	id string,
	rowAutomated, want bool,
) ([]string, []string) {
	if want && !rowAutomated {
		setIDs = append(setIDs, id)
	} else if !want && rowAutomated {
		clearIDs = append(clearIDs, id)
	}
	return setIDs, clearIDs
}
