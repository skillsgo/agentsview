package ssh

import (
	"context"
	"io"

	"github.com/skillsgo/agentsview/internal/remotesync"
)

func extractTarStream(
	ctx context.Context, r io.Reader, dst string,
) (int, error) {
	return remotesync.ExtractTarStream(ctx, r, dst)
}
