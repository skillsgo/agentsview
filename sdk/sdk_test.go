package sdk

import (
	"context"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

func TestOpenAndClose(t *testing.T) {
	a, e := Open(Config{DatabasePath: filepath.Join(t.TempDir(), "sessions.db"), AgentDirs: map[string][]string{"claude": {}}})
	require.NoError(t, e)
	p, e := a.Sessions(context.Background(), SessionQuery{IncludeOneShot: true})
	require.NoError(t, e)
	require.Zero(t, p.Total)
	require.NoError(t, a.Close())
}
func TestOpenRequiresPath(t *testing.T) {
	a, e := Open(Config{})
	require.Nil(t, a)
	require.EqualError(t, e, "sdk: DatabasePath is required")
}
