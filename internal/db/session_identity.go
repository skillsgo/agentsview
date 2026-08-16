package db

import "github.com/skillsgo/agentsview/internal/parser"

// ApplyParsedSessionIdentity copies parser-owned session identity onto a DB session.
func ApplyParsedSessionIdentity(dst *Session, src parser.ParsedSession) {
	dst.Agent = string(src.Agent)
	dst.AgentLabel = src.AgentLabel
	dst.Entrypoint = src.Entrypoint
	dst.SessionKind = src.SessionKind
}
