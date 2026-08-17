package sdk

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOpenAndClose(t *testing.T) {
	a, e := Open(Config{DatabasePath: filepath.Join(t.TempDir(), "sessions.db"), AgentDirs: map[string][]string{"claude": {}}})
	require.NoError(t, e)
	p, e := a.Sessions(context.Background(), SessionQuery{IncludeOneShot: true})
	require.NoError(t, e)
	require.Zero(t, p.Total)
	require.NoError(t, a.Close())
}

func TestBackgroundSyncCompletesInitialPass(t *testing.T) {
	root := t.TempDir()
	archive, err := Open(Config{
		DatabasePath: filepath.Join(t.TempDir(), "sessions.db"),
		AgentDirs:    map[string][]string{"codex": {root}},
	})
	require.NoError(t, err)
	completed := make(chan error, 1)
	background, err := archive.StartBackgroundSync(t.Context(), BackgroundSyncOptions{
		PollInterval: time.Hour,
		OnComplete: func(_ SyncResult, syncErr error) {
			completed <- syncErr
		},
	})
	require.NoError(t, err)
	require.NoError(t, <-completed)
	background.Close()
	require.NoError(t, archive.Close())
}

func TestOpenRequiresPath(t *testing.T) {
	a, e := Open(Config{})
	require.Nil(t, a)
	require.EqualError(t, e, "sdk: DatabasePath is required")
}
