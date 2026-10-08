// Package web holds the static files of the live cluster view served by
// cmd/raftkv-dash. Plain HTML, CSS and JavaScript; no build step.
package web

import "embed"

// Files is the dashboard's static content.
//
//go:embed index.html
var Files embed.FS
