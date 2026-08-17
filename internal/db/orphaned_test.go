package db

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopySyncStatePreservesArtifactImportAuthority(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	source := testDBAtPath(t, sourcePath, "source")
	work := artifactImportTestWork("peer-a1b2c3", 2)
	require.NoError(t, source.EnqueueArtifactImport(ctx, work))
	attempt, err := source.ReserveArtifactImportAttemptGeneration(ctx)
	require.NoError(t, err)
	pending, err := source.PendingArtifactImports(
		ctx,
		ArtifactImportVersions{Checkpoint: 1, Manifest: 2, Segment: 1},
		attempt,
		10,
	)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	marked, err := source.MarkArtifactImportAttempted(
		ctx, pending[0], attempt,
	)
	require.NoError(t, err)
	require.True(t, marked)
	marked, err = source.MarkArtifactImportQuarantinePending(ctx, pending[0])
	require.NoError(t, err)
	require.True(t, marked)

	head := ArtifactPeerCheckpointHead{
		Origin:           work.Origin,
		Sequence:         2,
		CheckpointSHA256: strings.Repeat("d", 64),
		CheckpointSize:   99,
	}
	_, err = source.RecordArtifactPeerCheckpointHead(ctx, head)
	require.NoError(t, err)
	landing := ArtifactCheckpointLanding(head)
	sessionMap := map[string]string{
		head.Origin + "~one": strings.Repeat("e", 64),
	}
	imported := ArtifactImportedSession{
		Origin:            head.Origin,
		GID:               head.Origin + "~one",
		ManifestHash:      sessionMap[head.Origin+"~one"],
		ImportedSessionID: head.Origin + "~one",
	}
	require.NoError(t, source.RecordArtifactImportedSession(ctx, imported))
	require.NoError(t, source.BeginArtifactCheckpointStage(ctx, landing, 1))
	require.NoError(t, source.StageArtifactCheckpointSessions(
		ctx, landing, []ArtifactCheckpointSession{{
			GID: imported.GID, ManifestHash: imported.ManifestHash,
		}},
	))
	require.NoError(t, source.CompleteArtifactCheckpointStage(
		ctx, landing, 1,
	))
	require.NoError(t, source.RecordArtifactCheckpointLandingFromStage(
		ctx, landing,
	))
	partial := ArtifactCheckpointLanding{
		Origin: "peer-b2c3d4", Sequence: 1,
		CheckpointSHA256: strings.Repeat("f", 64),
		CheckpointSize:   88,
	}
	_, err = source.RecordArtifactPeerCheckpointHead(
		ctx, ArtifactPeerCheckpointHead(partial),
	)
	require.NoError(t, err)
	require.NoError(t, source.BeginArtifactCheckpointStage(ctx, partial, 2))
	require.NoError(t, source.StageArtifactCheckpointSessionPage(
		ctx, partial,
		[]ArtifactCheckpointSession{{
			GID:          partial.Origin + "~partial",
			ManifestHash: strings.Repeat("1", 64),
		}},
		0, 42,
	))
	require.NoError(t, source.Close())

	destination := testDBAtPath(
		t, filepath.Join(dir, "destination.db"), "destination",
	)
	defer destination.Close()
	require.NoError(t, destination.CopySyncStateFrom(sourcePath))

	nextAttempt, err := destination.ReserveArtifactImportAttemptGeneration(ctx)
	require.NoError(t, err)
	assert.Greater(t, nextAttempt, attempt)
	pending, err = destination.PendingArtifactImports(
		ctx,
		ArtifactImportVersions{Checkpoint: 1, Manifest: 2, Segment: 1},
		nextAttempt,
		10,
	)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	assert.Equal(t, work.Name, pending[0].Name)
	assert.True(t, pending[0].QuarantinePending)

	gotHead, found, err := destination.GetArtifactPeerCheckpointHead(
		ctx, head.Origin,
	)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, head, gotHead)
	gotLanding, gotMap, found, err :=
		destination.GetArtifactCheckpointLanding(ctx, head.Origin)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, landing, gotLanding)
	assert.Equal(t, sessionMap, gotMap)
	gotProvenance, err := destination.ArtifactImportedManifestHashes(
		ctx, head.Origin, []string{imported.GID},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{
		imported.GID: imported.ManifestHash,
	}, gotProvenance)
	require.NoError(t, destination.BeginArtifactCheckpointStage(ctx, partial, 2))
	progress, err := destination.ArtifactCheckpointStageProgress(ctx, partial)
	require.NoError(t, err)
	assert.False(t, progress.Complete)
	assert.Equal(t, 1, progress.DecodedCount)
	assert.Equal(t, int64(42), progress.DecodeOffset)
}

func TestFullResyncPreservesImportedSessionUsage(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	source := testDBAtPath(t, sourcePath, "source")
	origin := "peer-a1b2c3"
	gid := origin + "~native-session"
	insertSession(t, source, gid, "project", func(session *Session) {
		session.Machine = origin
	})
	ordinal := 2
	require.NoError(t, source.ReplaceSessionUsageEvents(gid, []UsageEvent{{
		SessionID:                gid,
		MessageOrdinal:           &ordinal,
		Source:                   "artifact",
		Model:                    "example-model",
		InputTokens:              101,
		OutputTokens:             29,
		CacheCreationInputTokens: 11,
		CacheReadInputTokens:     7,
		ReasoningTokens:          5,
		CostStatus:               "reported",
		CostSource:               "provider",
		OccurredAt:               "2026-07-29T12:00:00Z",
		DedupKey:                 "imported-usage",
	}}))

	head := ArtifactPeerCheckpointHead{
		Origin:           origin,
		Sequence:         3,
		CheckpointSHA256: strings.Repeat("a", 64),
		CheckpointSize:   123,
	}
	_, err := source.RecordArtifactPeerCheckpointHead(ctx, head)
	require.NoError(t, err)
	manifestHash := strings.Repeat("b", 64)
	require.NoError(t, source.RecordArtifactCheckpointLanding(
		ctx,
		ArtifactCheckpointLanding(head),
		map[string]string{gid: manifestHash},
	))
	require.NoError(t, source.RecordArtifactImportedSession(
		ctx,
		ArtifactImportedSession{
			Origin:            origin,
			GID:               gid,
			ManifestHash:      manifestHash,
			ImportedSessionID: gid,
		},
	))
	require.NoError(t, source.Close())

	destination := testDBAtPath(
		t, filepath.Join(dir, "destination.db"), "destination",
	)
	defer destination.Close()
	require.NoError(t, destination.CopySyncStateFrom(sourcePath))
	copied, err := destination.CopyOrphanedDataFrom(sourcePath)
	require.NoError(t, err)
	require.Equal(t, 1, copied)

	events, err := destination.GetUsageEvents(ctx, gid)
	require.NoError(t, err)
	require.Len(t, events, 1)
	events[0].ID = 0
	assert.Equal(t, UsageEvent{
		SessionID:                gid,
		MessageOrdinal:           &ordinal,
		Source:                   "artifact",
		Model:                    "example-model",
		InputTokens:              101,
		OutputTokens:             29,
		CacheCreationInputTokens: 11,
		CacheReadInputTokens:     7,
		ReasoningTokens:          5,
		CostStatus:               "reported",
		CostSource:               "provider",
		OccurredAt:               "2026-07-29T12:00:00Z",
		DedupKey:                 "imported-usage",
	}, events[0])

	provenance, err := destination.ArtifactImportedManifestHashes(
		ctx, origin, []string{gid},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{gid: manifestHash}, provenance)
}

func TestCopySyncStateAcceptsDatabaseWithoutArtifactImportTables(t *testing.T) {
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "old.db")
	source, err := sql.Open("sqlite3", makeDSN(sourcePath, false))
	require.NoError(t, err)
	_, err = source.Exec(`CREATE TABLE pg_sync_state (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL
	)`)
	require.NoError(t, err)
	require.NoError(t, source.Close())

	destination := testDBAtPath(
		t, filepath.Join(dir, "destination.db"), "destination",
	)
	defer destination.Close()
	require.NoError(t, destination.CopySyncStateFrom(sourcePath))
}

func TestCopySyncStateRejectsEqualLandingWithDifferentMap(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	source := testDBAtPath(t, sourcePath, "source")
	head := ArtifactPeerCheckpointHead{
		Origin:           "peer-a1b2c3",
		Sequence:         2,
		CheckpointSHA256: strings.Repeat("a", 64),
		CheckpointSize:   42,
	}
	_, err := source.RecordArtifactPeerCheckpointHead(ctx, head)
	require.NoError(t, err)
	require.NoError(t, source.RecordArtifactCheckpointLanding(
		ctx,
		ArtifactCheckpointLanding(head),
		map[string]string{head.Origin + "~source": strings.Repeat("b", 64)},
	))
	require.NoError(t, source.Close())

	destination := testDBAtPath(
		t, filepath.Join(dir, "destination.db"), "destination",
	)
	defer destination.Close()
	_, err = destination.RecordArtifactPeerCheckpointHead(ctx, head)
	require.NoError(t, err)
	require.NoError(t, destination.RecordArtifactCheckpointLanding(
		ctx,
		ArtifactCheckpointLanding(head),
		map[string]string{head.Origin + "~destination": strings.Repeat("c", 64)},
	))

	err = destination.CopySyncStateFrom(sourcePath)
	require.ErrorIs(t, err, ErrArtifactImportConflict)
}

func TestCopySyncStateRejectsIncompatibleCheckpointStages(t *testing.T) {
	tests := []struct {
		name               string
		sourceEntries      []ArtifactCheckpointSession
		destinationEntries []ArtifactCheckpointSession
		sourceOffset       int64
		destinationOffset  int64
		complete           bool
	}{
		{
			name: "complete stages have different session sets",
			sourceEntries: []ArtifactCheckpointSession{
				{GID: "peer-a1b2c3~one", ManifestHash: strings.Repeat("1", 64)},
				{GID: "peer-a1b2c3~two", ManifestHash: strings.Repeat("2", 64)},
			},
			destinationEntries: []ArtifactCheckpointSession{
				{GID: "peer-a1b2c3~one", ManifestHash: strings.Repeat("1", 64)},
				{GID: "peer-a1b2c3~three", ManifestHash: strings.Repeat("3", 64)},
			},
			complete: true,
		},
		{
			name: "partial stages have different decoded prefixes",
			sourceEntries: []ArtifactCheckpointSession{
				{GID: "peer-a1b2c3~one", ManifestHash: strings.Repeat("1", 64)},
			},
			destinationEntries: []ArtifactCheckpointSession{
				{GID: "peer-a1b2c3~two", ManifestHash: strings.Repeat("2", 64)},
			},
			sourceOffset:      10,
			destinationOffset: 10,
		},
		{
			name: "partial stage count and cursor progress cross",
			sourceEntries: []ArtifactCheckpointSession{
				{GID: "peer-a1b2c3~one", ManifestHash: strings.Repeat("1", 64)},
				{GID: "peer-a1b2c3~two", ManifestHash: strings.Repeat("2", 64)},
			},
			destinationEntries: []ArtifactCheckpointSession{
				{GID: "peer-a1b2c3~one", ManifestHash: strings.Repeat("1", 64)},
			},
			sourceOffset:      10,
			destinationOffset: 20,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			sourcePath := filepath.Join(dir, "source.db")
			source := testDBAtPath(t, sourcePath, "source")
			landing := ArtifactCheckpointLanding{
				Origin:           "peer-a1b2c3",
				Sequence:         4,
				CheckpointSHA256: strings.Repeat("a", 64),
				CheckpointSize:   91,
			}
			stageCheckpointForCopyTest(
				t, source, landing, tc.sourceEntries,
				tc.sourceOffset, tc.complete,
			)
			require.NoError(t, source.Close())

			destination := testDBAtPath(
				t, filepath.Join(dir, "destination.db"), "destination",
			)
			defer destination.Close()
			stageCheckpointForCopyTest(
				t, destination, landing, tc.destinationEntries,
				tc.destinationOffset, tc.complete,
			)

			err := destination.CopySyncStateFrom(sourcePath)
			require.ErrorIs(t, err, ErrArtifactImportConflict)
		})
	}
}

func TestCopySyncStateMergesCompatibleCheckpointStagePrefix(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	sourcePath := filepath.Join(dir, "source.db")
	source := testDBAtPath(t, sourcePath, "source")
	landing := ArtifactCheckpointLanding{
		Origin:           "peer-a1b2c3",
		Sequence:         4,
		CheckpointSHA256: strings.Repeat("a", 64),
		CheckpointSize:   91,
	}
	one := ArtifactCheckpointSession{
		GID: landing.Origin + "~one", ManifestHash: strings.Repeat("1", 64),
	}
	two := ArtifactCheckpointSession{
		GID: landing.Origin + "~two", ManifestHash: strings.Repeat("2", 64),
	}
	stageCheckpointForCopyTest(
		t, source, landing, []ArtifactCheckpointSession{one, two}, 20, false,
	)
	require.NoError(t, source.Close())

	destination := testDBAtPath(
		t, filepath.Join(dir, "destination.db"), "destination",
	)
	defer destination.Close()
	stageCheckpointForCopyTest(
		t, destination, landing, []ArtifactCheckpointSession{one}, 10, false,
	)

	require.NoError(t, destination.CopySyncStateFrom(sourcePath))
	progress, err := destination.ArtifactCheckpointStageProgress(ctx, landing)
	require.NoError(t, err)
	assert.False(t, progress.Complete)
	assert.Equal(t, 2, progress.DecodedCount)
	assert.Equal(t, int64(20), progress.DecodeOffset)
	var stagedCount int
	require.NoError(t, destination.getReader().QueryRowContext(ctx, `
		SELECT count(*)
		FROM artifact_checkpoint_stage_sessions
		WHERE origin = ? AND sequence = ?`,
		landing.Origin, landing.Sequence,
	).Scan(&stagedCount))
	assert.Equal(t, 2, stagedCount)
}

func stageCheckpointForCopyTest(
	t *testing.T,
	database *DB,
	landing ArtifactCheckpointLanding,
	entries []ArtifactCheckpointSession,
	offset int64,
	complete bool,
) {
	t.Helper()
	ctx := t.Context()
	require.NoError(t, database.BeginArtifactCheckpointStage(ctx, landing, 1))
	if complete {
		require.NoError(t, database.StageArtifactCheckpointSessions(
			ctx, landing, entries,
		))
		require.NoError(t, database.CompleteArtifactCheckpointStage(
			ctx, landing, len(entries),
		))
		return
	}
	require.NoError(t, database.StageArtifactCheckpointSessionPage(
		ctx, landing, entries, 0, offset,
	))
}

func TestExecWithoutCancelDropsTempTableWithCanceledContext(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.db")
	pool, err := sql.Open("sqlite3", path)
	require.NoError(t, err, "open sqlite")
	defer pool.Close()

	baseCtx := context.Background()
	conn, err := pool.Conn(baseCtx)
	require.NoError(t, err, "pin sqlite connection")
	defer conn.Close()

	_, err = conn.ExecContext(baseCtx, `
		CREATE TEMP TABLE _test_cleanup (
			id TEXT PRIMARY KEY
		)`)
	require.NoError(t, err, "create temp table")

	ctx, cancel := context.WithCancel(baseCtx)
	cancel()

	_, err = execWithoutCancel(ctx, conn,
		"DROP TABLE IF EXISTS _test_cleanup")
	require.NoError(t, err, "drop with canceled context")

	_, err = conn.ExecContext(baseCtx, `
		CREATE TEMP TABLE _test_cleanup (
			id TEXT PRIMARY KEY
		)`)
	require.NoError(t, err, "recreate temp table after cleanup")
}

func TestCopyOrphanedDataPreservesSessionKindAndPromptSource(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "old.db")
	srcDB := testDBAtPath(t, srcPath, "src")
	insertSession(t, srcDB, "kind-orphan", "proj", func(s *Session) {
		s.SessionKind = "bg"
		s.MessageCount = 2
	})
	insertMessages(t, srcDB,
		Message{
			SessionID: "kind-orphan", Ordinal: 0, Role: "user",
			Content: "first", PromptSource: "typed",
		},
		Message{
			SessionID: "kind-orphan", Ordinal: 1, Role: "user",
			Content: "second", PromptSource: "queued",
		},
	)
	require.NoError(t, srcDB.Close(), "close source")

	dstPath := filepath.Join(dir, "new.db")
	dstDB := testDBAtPath(t, dstPath, "dst")
	defer dstDB.Close()

	count, err := dstDB.CopyOrphanedDataFrom(srcPath)
	require.NoError(t, err, "CopyOrphanedDataFrom")
	require.Equal(t, 1, count, "expected one orphan")

	session, err := dstDB.GetSession(ctx, "kind-orphan")
	require.NoError(t, err, "get copied session")
	assert.Equal(t, "bg", session.SessionKind)

	msgs, err := dstDB.GetMessages(ctx, "kind-orphan", 0, 10, true)
	require.NoError(t, err, "get copied messages")
	require.Len(t, msgs, 2)
	assert.Equal(t, "typed", msgs[0].PromptSource)
	assert.Equal(t, "queued", msgs[1].PromptSource)
}
