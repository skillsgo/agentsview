// Package backendcontract centralizes compile-time checks that every storage
// backend implements the full server-facing db.Store capability surface.
package backendcontract

import (
	"github.com/skillsgo/agentsview/internal/db"
	duckdbstore "github.com/skillsgo/agentsview/internal/duckdb"
	postgresstore "github.com/skillsgo/agentsview/internal/postgres"
)

var (
	_ db.Store = (*db.DB)(nil)
	_ db.Store = (*postgresstore.Store)(nil)
	_ db.Store = (*duckdbstore.Store)(nil)
)
