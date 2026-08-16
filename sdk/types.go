package sdk

type SessionQuery struct {
	Project, Machine, Agent, DateFrom, DateTo, Timezone, Cursor string
	IncludeOneShot, IncludeAutomated, IncludeChildren           bool
	Limit                                                       int
}
type SessionPage struct {
	Sessions   []Session `json:"sessions"`
	NextCursor string    `json:"next_cursor,omitempty"`
	Total      int       `json:"total"`
}
type Session struct {
	ID, Project, Machine, Agent                   string
	FirstMessage, DisplayName, StartedAt, EndedAt *string
	MessageCount, UserMessageCount                int
	Cwd, GitBranch, CreatedAt                     string
}
type MessageQuery struct {
	From      *int
	Limit     int
	Direction string
	Roles     []string
}
type MessagePage struct {
	Messages                  []Message
	Count                     int
	FirstOrdinal, LastOrdinal *int
}
type Message struct {
	Ordinal                         int
	Role, Content, Timestamp, Model string
	ToolCalls                       []ToolCall
}
type ToolCall struct{ ToolName, Category, SkillName string }
type SkillUsageQuery struct{ From, To, Machine, Project, Agent, Model, Timezone, Granularity string }
type SkillUsageReport struct {
	TotalSkillCalls, DistinctSkills int
	BySkill                         []SkillUsage
}
type SkillUsage struct {
	SkillName               string
	CallCount, SessionCount int
	LastUsedAt              string
}
type SyncResult struct {
	TotalSessions, Synced, Skipped, Failed, Tombstoned int
	Aborted                                            bool
	Warnings                                           []string
}
