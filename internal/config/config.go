package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	RTMPAddr      string
	HTTPAddr      string
	App           string
	PublishKey    string
	MaxPublishers int
	MaxPlayers    int
	ReadTimeoutS  int
	WriteTimeoutS int
	ChunkSize     int
	GOPCache      bool
	LogAccess     bool
}

func Load(path string) (*Config, error) {
	c := &Config{
		RTMPAddr:      ":1935",
		HTTPAddr:      ":8080",
		App:           "live",
		PublishKey:    "",
		MaxPublishers: 128,
		MaxPlayers:    4096,
		ReadTimeoutS:  30,
		WriteTimeoutS: 30,
		ChunkSize:     4096,
		GOPCache:      true,
		LogAccess:     true,
	}

	if path != "" {
		if err := applyFile(c, path); err != nil {
			return nil, err
		}
	}
	applyEnv(c)
	return c, nil
}

func applyFile(c *Config, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		k := strings.TrimSpace(line[:eq])
		v := strings.TrimSpace(line[eq+1:])
		v = strings.Trim(v, `"'`)
		assign(c, k, v)
	}
	return s.Err()
}

func applyEnv(c *Config) {
	for _, k := range []string{
		"RTMP_ADDR", "HTTP_ADDR", "RTMP_APP", "PUBLISH_KEY",
		"MAX_PUBLISHERS", "MAX_PLAYERS",
		"READ_TIMEOUT", "WRITE_TIMEOUT",
		"CHUNK_SIZE", "GOP_CACHE", "LOG_ACCESS",
	} {
		if v, ok := os.LookupEnv(k); ok {
			assign(c, k, v)
		}
	}
}

func assign(c *Config, k, v string) {
	switch strings.ToUpper(k) {
	case "RTMP_ADDR":
		c.RTMPAddr = v
	case "HTTP_ADDR":
		c.HTTPAddr = v
	case "RTMP_APP", "APP":
		c.App = v
	case "PUBLISH_KEY":
		c.PublishKey = v
	case "MAX_PUBLISHERS":
		if n, err := strconv.Atoi(v); err == nil {
			c.MaxPublishers = n
		}
	case "MAX_PLAYERS":
		if n, err := strconv.Atoi(v); err == nil {
			c.MaxPlayers = n
		}
	case "READ_TIMEOUT":
		if n, err := strconv.Atoi(v); err == nil {
			c.ReadTimeoutS = n
		}
	case "WRITE_TIMEOUT":
		if n, err := strconv.Atoi(v); err == nil {
			c.WriteTimeoutS = n
		}
	case "CHUNK_SIZE":
		if n, err := strconv.Atoi(v); err == nil && n >= 128 && n <= 65536 {
			c.ChunkSize = n
		}
	case "GOP_CACHE":
		c.GOPCache = parseBool(v)
	case "LOG_ACCESS":
		c.LogAccess = parseBool(v)
	}
}

func parseBool(v string) bool {
	switch strings.ToLower(v) {
	case "1", "t", "true", "yes", "on":
		return true
	}
	return false
}
