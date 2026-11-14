// Package config provides startup configuration helpers including 12-factor .env file loading.
package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const defaultEnvFile = ".env"

// LoadDotEnv loads KEY=VALUE pairs from path (or ENV_FILE / ".env" when path is empty)
// into the process environment without overwriting already-defined environment variables.
// A missing file is treated as a no-op so production containers without a .env file run cleanly.
func LoadDotEnv(path string) error {
	target := strings.TrimSpace(path)
	if target == "" {
		if fromEnv := strings.TrimSpace(os.Getenv("ENV_FILE")); fromEnv != "" {
			target = fromEnv
		} else {
			target = defaultEnvFile
		}
	}

	// #nosec G304 -- target is a local startup config path (.env or ENV_FILE), never user request input.
	f, err := os.Open(target)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open env file %q: %w", target, err)
	}
	defer func() {
		_ = f.Close()
	}()

	scanner := bufio.NewScanner(f)
	// Support large JSON values (e.g. AUTH_STATIC_TOKENS_JSON) up to 1 MiB per line.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(strings.TrimSuffix(scanner.Text(), "\r"))
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}

		idx := strings.IndexByte(line, '=')
		if idx <= 0 {
			return fmt.Errorf("invalid env syntax at %s:%d", target, lineNum)
		}

		key := strings.TrimSpace(line[:idx])
		if key == "" || strings.ContainsAny(key, " \t") {
			return fmt.Errorf("invalid env key at %s:%d", target, lineNum)
		}

		val, err := parseDotEnvValue(line[idx+1:])
		if err != nil {
			return fmt.Errorf("invalid env value for %s at %s:%d: %w", key, target, lineNum, err)
		}

		if _, exists := os.LookupEnv(key); !exists {
			if err := os.Setenv(key, val); err != nil {
				return fmt.Errorf("set env %s: %w", key, err)
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan env file %q: %w", target, err)
	}
	return nil
}

func parseDotEnvValue(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}

	quote := trimmed[0]
	if quote == '"' || quote == '\'' {
		end := findClosingQuote(trimmed, quote)
		if end == -1 {
			return "", errors.New("unterminated quoted value")
		}
		inner := trimmed[1:end]
		if quote == '\'' {
			return inner, nil
		}
		unescaped, err := strconv.Unquote(`"` + escapeUnescapedQuotes(inner) + `"`)
		if err != nil {
			return inner, nil
		}
		return unescaped, nil
	}

	if commentIdx := findUnquotedComment(trimmed); commentIdx >= 0 {
		trimmed = strings.TrimSpace(trimmed[:commentIdx])
	}
	return trimmed, nil
}

func findClosingQuote(s string, quote byte) int {
	escaped := false
	for i := 1; i < len(s); i++ {
		ch := s[i]
		if quote == '"' && ch == '\\' && !escaped {
			escaped = true
			continue
		}
		if ch == quote && !escaped {
			return i
		}
		escaped = false
	}
	return -1
}

func escapeUnescapedQuotes(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	escaped := false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if ch == '"' && !escaped {
			b.WriteString(`\"`)
		} else {
			b.WriteByte(ch)
		}
		if ch == '\\' && !escaped {
			escaped = true
		} else {
			escaped = false
		}
	}
	return b.String()
}

func findUnquotedComment(s string) int {
	for i := 0; i < len(s); i++ {
		if s[i] == '#' && i > 0 && (s[i-1] == ' ' || s[i-1] == '\t') {
			return i
		}
	}
	return -1
}
