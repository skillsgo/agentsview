//go:build !cgo

package db

import (
	"database/sql"

	modernsqlite "modernc.org/sqlite"
)

const (
	sqliteDriverName      = "sqlite"
	sqliteUsageDriverName = "agentsview_sqlite3"
)

func init() {
	sql.Register(sqliteUsageDriverName, &modernsqlite.Driver{})
}

func sqliteUsageOutputTokens(tokenJSON string) int {
	_, outputTokens, _, _ := parseUsageTokenCounters(tokenJSON)
	return outputTokens
}
