package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/skillsgo/agentsview/internal/config"
	"github.com/skillsgo/agentsview/internal/db"
	"github.com/spf13/cobra"
)

func newStorageCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "storage",
		Short:   "Inspect experimental archive storage",
		GroupID: groupData,
		Args:    cobra.NoArgs,
	}
	cmd.AddCommand(newStorageCompareCommand())
	return cmd
}

func newStorageCompareCommand() *cobra.Command {
	var destination string
	var verify bool
	cmd := &cobra.Command{
		Use:   "compare",
		Short: "Build and measure an Agent-aware compressed content archive",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if destination == "" {
				return errors.New("--archive is required")
			}
			cfg, err := config.LoadPFlags(cmd.Flags())
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}
			source, err := db.OpenReadOnly(cfg.DBPath)
			if err != nil {
				return fmt.Errorf("opening source archive: %w", err)
			}
			defer source.Close()
			report, err := source.BuildContentArchive(cmd.Context(), destination)
			if err != nil {
				return err
			}
			if verify {
				if err := db.VerifyContentArchive(cmd.Context(), destination); err != nil {
					return err
				}
			}
			if outputFormat(cmd) == "json" {
				return json.NewEncoder(cmd.OutOrStdout()).Encode(report)
			}
			return printContentArchiveReport(cmd.OutOrStdout(), report, verify)
		},
	}
	cmd.Flags().StringVar(
		&destination, "archive", "",
		"Fresh destination path for the compressed comparison archive",
	)
	cmd.Flags().BoolVar(
		&verify, "verify", true,
		"Reconstruct and hash every unique content object after building",
	)
	registerFormatFlags(cmd.Flags())
	return cmd
}

func printContentArchiveReport(
	w io.Writer, report db.ContentArchiveReport, verified bool,
) error {
	write := func(format string, args ...any) error {
		_, err := fmt.Fprintf(w, format, args...)
		return err
	}
	if err := write("Agent-aware content archive comparison\n\n"); err != nil {
		return err
	}
	rows := []struct {
		label string
		value string
	}{
		{"Legacy SQLite file", formatBytes(report.SourceBytes)},
		{"Compressed content archive", formatBytes(report.ArchiveBytes)},
		{"Content references", fmt.Sprintf("%d", report.References)},
		{"Unique content objects", fmt.Sprintf("%d", report.UniqueObjects)},
		{"Unique chunks", fmt.Sprintf("%d", report.UniqueChunks)},
		{"Referenced raw content", formatBytes(report.ReferencedRawBytes)},
		{"Unique raw content", formatBytes(report.UniqueRawBytes)},
		{"Compressed chunk payload", formatBytes(report.CompressedChunkBytes)},
		{"Duplicate bytes eliminated", formatBytes(report.DuplicateBytesEliminated)},
		{"Build duration", report.BuildDuration.Round(time.Millisecond).String()},
		{"Full reconstruction verified", fmt.Sprintf("%t", verified)},
	}
	for _, row := range rows {
		if err := write("%-30s %s\n", row.label+":", row.value); err != nil {
			return err
		}
	}
	if err := write("\nBy Agent content role:\n"); err != nil {
		return err
	}
	fields := make([]string, 0, len(report.ByField))
	for field := range report.ByField {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	for _, field := range fields {
		stats := report.ByField[field]
		if err := write("  %-28s %8d refs  %s\n",
			field, stats.References, formatBytes(stats.RawBytes)); err != nil {
			return err
		}
	}
	return nil
}
