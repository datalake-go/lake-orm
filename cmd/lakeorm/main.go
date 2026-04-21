// Command lakeorm is the lakeorm CLI. v0: ships `version` and
// stubs for `migrate` and `stack up`. v1 adds lakeorm.toml parsing,
// `lakeorm migrate plan|apply`, `lakeorm stack up|down`, and
// `lakeorm doctor`.
package main

import (
	"fmt"
	"os"
)

// Version is set at link time (e.g. -ldflags "-X main.Version=...").
var Version = "v0.0.0-dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "version", "-v", "--version":
		fmt.Println(Version)
	case "migrate":
		fmt.Fprintln(os.Stderr, "lakeorm: migrate subcommand is a v1 target — lake-goose runs migrations today; lakeorm.MigrateGenerate authors them")
		os.Exit(1)
	case "stack":
		fmt.Fprintln(os.Stderr, "lakeorm: stack subcommand is a v1 target — use `make docker-up` in the repo root")
		os.Exit(1)
	case "doctor":
		fmt.Fprintln(os.Stderr, "lakeorm: doctor subcommand is a v1 target — call lakeorm.Verify(ctx, db) from your code")
		os.Exit(1)
	case "help", "-h", "--help":
		usage()
	default:
		fmt.Fprintf(os.Stderr, "lakeorm: unknown subcommand %q\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `lakeorm — lakeorm CLI

Usage:
  lakeorm version         Print the CLI version
  lakeorm migrate         [v1] Read lakeorm.toml, diff/apply schema changes
  lakeorm stack up|down   [v1] Bring the local lake-k8s stack up or down
  lakeorm doctor          [v1] Run lakeorm.Verify against the configured client

See https://github.com/datalake-go/lake-orm for docs.
`)
}
