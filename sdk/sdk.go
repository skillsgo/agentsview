package sdk

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/skillsgo/agentsview/internal/config"
	"github.com/skillsgo/agentsview/internal/db"
	"github.com/skillsgo/agentsview/internal/parser"
	"github.com/skillsgo/agentsview/internal/service"
	avsync "github.com/skillsgo/agentsview/internal/sync"
)

type Config struct {
	DatabasePath   string
	Machine        string
	AgentDirs      map[string][]string
	DisabledAgents []string
}
type Archive struct {
	db      *db.DB
	engine  *avsync.Engine
	service service.SessionService
	roots   []string
}

type BackgroundSyncOptions struct {
	Debounce     time.Duration
	PollInterval time.Duration
	OnComplete   func(SyncResult, error)
}

type BackgroundSyncStatus struct {
	State               string
	ConsecutiveFailures int
	LastError           string
}

type BackgroundSync struct {
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
	mu     sync.RWMutex
	status BackgroundSyncStatus
}

func Open(c Config) (*Archive, error) {
	if strings.TrimSpace(c.DatabasePath) == "" {
		return nil, errors.New("sdk: DatabasePath is required")
	}
	d, err := config.Default()
	if err != nil {
		return nil, fmt.Errorf("sdk: defaults: %w", err)
	}
	disabled, err := config.NormalizeDisabledAgents(c.DisabledAgents)
	if err != nil {
		return nil, fmt.Errorf("sdk: %w", err)
	}
	for name, dirs := range c.AgentDirs {
		agent, ok := knownAgent(name)
		if !ok {
			return nil, fmt.Errorf("sdk: unknown agent %q", name)
		}
		d.AgentDirs[agent] = append([]string(nil), dirs...)
	}
	machine := strings.TrimSpace(c.Machine)
	if machine == "" {
		machine = d.LocalMachineName
	}
	d.DisabledAgents = disabled
	database, err := db.Open(c.DatabasePath)
	if err != nil {
		return nil, fmt.Errorf("sdk: open archive: %w", err)
	}
	engine := avsync.NewEngine(database, avsync.EngineConfig{AgentDirs: d.AgentDirs, SourceMachines: d.SourceMachines, DisabledAgents: disabled, Machine: machine, BlockedResultCategories: d.ResultContentBlockedCategories, ProviderFactories: d.LocalProviderFactories()})
	var roots []string
	seenRoots := map[string]struct{}{}
	for _, dirs := range d.AgentDirs {
		for _, dir := range dirs {
			dir = strings.TrimSpace(dir)
			if dir == "" {
				continue
			}
			if _, exists := seenRoots[dir]; exists {
				continue
			}
			seenRoots[dir] = struct{}{}
			roots = append(roots, dir)
		}
	}
	return &Archive{database, engine, service.NewDirectBackend(database, engine), roots}, nil
}
func knownAgent(name string) (parser.AgentType, bool) {
	want := parser.AgentType(strings.ToLower(strings.TrimSpace(name)))
	for _, d := range parser.Registry {
		if d.Type == want {
			return want, true
		}
	}
	return "", false
}
func (a *Archive) ready() error {
	if a == nil || a.db == nil {
		return errors.New("sdk: archive is closed")
	}
	return nil
}
func (a *Archive) Close() error {
	if a == nil || a.db == nil {
		return nil
	}
	a.engine.Close()
	err := a.db.Close()
	a.db = nil
	return err
}
func (a *Archive) Sync(ctx context.Context) (SyncResult, error) {
	if err := a.ready(); err != nil {
		return SyncResult{}, err
	}
	s := a.engine.SyncAll(ctx, nil)
	r := SyncResult{s.TotalSessions, s.Synced, s.Skipped, s.Failed, s.Tombstoned, s.Aborted, append([]string(nil), s.Warnings...)}
	return r, ctx.Err()
}

// StartBackgroundSync starts one concurrent initial archive sync, collects
// filesystem events while it runs, and then applies bounded incremental
// batches for the lifetime of ctx. Polling is retained as a correctness
// fallback for missing roots and watcher coverage loss.
func (a *Archive) StartBackgroundSync(
	ctx context.Context, options BackgroundSyncOptions,
) (*BackgroundSync, error) {
	if err := a.ready(); err != nil {
		return nil, err
	}
	if options.Debounce <= 0 {
		options.Debounce = 300 * time.Millisecond
	}
	if options.PollInterval <= 0 {
		options.PollInterval = 5 * time.Minute
	}
	backgroundCtx, cancel := context.WithCancel(ctx)
	notify := func(result SyncResult, err error) {
		if options.OnComplete != nil {
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						err = fmt.Errorf("sdk: background callback panic: %v", recovered)
					}
				}()
				options.OnComplete(result, err)
			}()
		}
	}
	background := &BackgroundSync{cancel: cancel, done: make(chan struct{})}
	background.setStatus("starting", 0, nil)
	go func() {
		defer close(background.done)
		failures := 0
		for backgroundCtx.Err() == nil {
			err := a.runBackgroundGeneration(backgroundCtx, options, notify, background)
			if backgroundCtx.Err() != nil {
				break
			}
			failures++
			background.setStatus("retrying", failures, err)
			notify(SyncResult{}, err)
			delay := time.Second << min(failures-1, 6)
			timer := time.NewTimer(delay)
			select {
			case <-backgroundCtx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
		background.setStatus("stopped", failures, backgroundCtx.Err())
	}()
	return background, nil
}

func (a *Archive) runBackgroundGeneration(
	ctx context.Context, options BackgroundSyncOptions,
	notify func(SyncResult, error), background *BackgroundSync,
) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("sdk: background sync panic: %v", recovered)
		}
	}()
	recovery := &avsync.WatchRecoveryScope{AvailableRoots: append([]string(nil), a.roots...)}
	watcher, err := avsync.NewWatcherWithCallback(
		options.Debounce, options.Debounce,
		func(callbackCtx context.Context, batch avsync.WatchBatch) (callbackErr error) {
			defer func() {
				if recovered := recover(); recovered != nil {
					callbackErr = fmt.Errorf("sdk: watcher callback panic: %v", recovered)
				}
				notify(SyncResult{}, callbackErr)
			}()
			return avsync.ApplyWatchBatch(callbackCtx, a.engine, batch, recovery)
		}, nil, avsync.WatcherOptions{},
	)
	if err != nil {
		return fmt.Errorf("sdk: create archive watcher: %w", err)
	}
	defer watcher.Stop()
	for _, root := range a.roots {
		if _, statErr := os.Stat(root); statErr == nil {
			watcher.WatchRecursive(root)
		}
	}
	if err := watcher.StartCollecting(); err != nil {
		return fmt.Errorf("sdk: start archive watcher: %w", err)
	}
	result, err := a.Sync(ctx)
	notify(result, err)
	if err != nil {
		return err
	}
	background.setStatus("running", 0, nil)
	watcher.OpenDispatch()
	ticker := time.NewTicker(options.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-watcher.Done():
			return errors.New("sdk: archive watcher stopped unexpectedly")
		case <-ticker.C:
			result, err = a.Sync(ctx)
			notify(result, err)
			if err != nil {
				return err
			}
		}
	}
}

func (b *BackgroundSync) setStatus(state string, failures int, err error) {
	b.mu.Lock()
	b.status = BackgroundSyncStatus{State: state, ConsecutiveFailures: failures}
	if err != nil {
		b.status.LastError = err.Error()
	}
	b.mu.Unlock()
}

func (b *BackgroundSync) Status() BackgroundSyncStatus {
	if b == nil {
		return BackgroundSyncStatus{State: "stopped"}
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status
}

func (b *BackgroundSync) Close() {
	if b == nil {
		return
	}
	b.once.Do(func() { b.cancel() })
	<-b.done
}
func (a *Archive) Sessions(ctx context.Context, q SessionQuery) (SessionPage, error) {
	if err := a.ready(); err != nil {
		return SessionPage{}, err
	}
	p, err := a.service.List(ctx, service.ListFilter{Project: q.Project, Machine: q.Machine, Agent: q.Agent, DateFrom: q.DateFrom, DateTo: q.DateTo, Timezone: q.Timezone, IncludeOneShot: q.IncludeOneShot, IncludeAutomated: q.IncludeAutomated, IncludeChildren: q.IncludeChildren, Cursor: q.Cursor, Limit: q.Limit})
	if err != nil {
		return SessionPage{}, err
	}
	out := SessionPage{NextCursor: p.NextCursor, Total: p.Total}
	for _, s := range p.Sessions {
		out.Sessions = append(out.Sessions, mapSession(s))
	}
	return out, nil
}
func (a *Archive) Session(ctx context.Context, id string) (*Session, error) {
	if err := a.ready(); err != nil {
		return nil, err
	}
	d, err := a.service.Get(ctx, id)
	if err != nil || d == nil {
		return nil, err
	}
	s := mapSession(d.Session)
	return &s, nil
}
func (a *Archive) Messages(ctx context.Context, id string, q MessageQuery) (MessagePage, error) {
	if err := a.ready(); err != nil {
		return MessagePage{}, err
	}
	p, err := a.service.Messages(ctx, id, service.MessageFilter{From: q.From, Limit: q.Limit, Direction: q.Direction, Roles: q.Roles})
	if err != nil {
		return MessagePage{}, err
	}
	out := MessagePage{Count: p.Count, FirstOrdinal: p.FirstOrdinal, LastOrdinal: p.LastOrdinal}
	for _, m := range p.Messages {
		mm := Message{Ordinal: m.Ordinal, Role: m.Role, Content: m.Content, Timestamp: m.Timestamp, Model: m.Model}
		for _, c := range m.ToolCalls {
			mm.ToolCalls = append(mm.ToolCalls, ToolCall{c.ToolName, c.Category, c.SkillName})
		}
		out.Messages = append(out.Messages, mm)
	}
	return out, nil
}
func (a *Archive) SkillUsage(ctx context.Context, q SkillUsageQuery) (SkillUsageReport, error) {
	if err := a.ready(); err != nil {
		return SkillUsageReport{}, err
	}
	r, err := a.db.GetAnalyticsSkills(ctx, db.AnalyticsFilter{From: q.From, To: q.To, Machine: q.Machine, Project: q.Project, Agent: q.Agent, Model: q.Model, Timezone: q.Timezone}, q.Granularity)
	if err != nil {
		return SkillUsageReport{}, err
	}
	out := SkillUsageReport{TotalSkillCalls: r.TotalSkillCalls, DistinctSkills: r.DistinctSkills}
	for _, x := range r.BySkill {
		out.BySkill = append(out.BySkill, SkillUsage{x.SkillName, x.CallCount, x.SessionCount, x.LastUsedAt})
	}
	return out, nil
}
func mapSession(s db.Session) Session {
	return Session{ID: s.ID, Project: s.Project, Machine: s.Machine, Agent: s.Agent, FirstMessage: s.FirstMessage, DisplayName: s.DisplayName, StartedAt: s.StartedAt, EndedAt: s.EndedAt, MessageCount: s.MessageCount, UserMessageCount: s.UserMessageCount, Cwd: s.Cwd, GitBranch: s.GitBranch, CreatedAt: s.CreatedAt}
}
