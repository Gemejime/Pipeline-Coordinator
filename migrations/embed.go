// Package migrations embeds versioned SQL DDL migrations for coordinator startup and ArgoCD PreSync jobs.
package migrations

import "embed"

// FS holds all embedded SQL migration files.
//
//go:embed *.sql
var FS embed.FS
