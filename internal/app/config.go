package app

import (
	"encoding/json"
	"os"
	"sync"
)

type Config struct {
	Port           int    `json:"port"`
	Host           string `json:"host"`
	RetryAttempts  int    `json:"retry_attempts"`
	RetryDelaySec  int    `json:"retry_delay_sec"`
	RequestTimeout int    `json:"request_timeout_sec"`
	GeminiBL       string `json:"gemini_bl"`
	DefaultModel   string `json:"default_model"`
	LogRequests    bool   `json:"log_requests"`
	CookieFile     string `json:"cookie_file"`
	Proxy          string `json:"proxy"`
	ProxyPoolURL   string `json:"proxy_pool_url"`
	ProxyMode      string `json:"proxy_mode"`
	Impersonate    string `json:"impersonate"`
	DBPath         string `json:"db_path"`
	AdminToken     string `json:"admin_token"`
	AdminEnabled   bool   `json:"admin_enabled"`
	RetentionDays  int    `json:"retention_days"`

	// Per-IP rate limit (一个 slot = 直连/或一个代理),0 表示不限。
	// 默认值基于 Google 单 IP 实测容忍度：100+ 累积请求会触发 sorry/index 拦截。
	PerIPConcurrent int `json:"per_ip_concurrent"` // 瞬时并发上限
	PerIPRPM        int `json:"per_ip_rpm"`        // 每分钟请求上限
	PerIPRPH        int `json:"per_ip_rph"`        // 每小时请求上限
}

var (
	cfg     Config
	cfgOnce sync.Once
)

func defaultConfig() Config {
	return Config{
		Port:           8083,
		Host:           "0.0.0.0",
		RetryAttempts:  3,
		RetryDelaySec:  2,
		RequestTimeout: 180,
		GeminiBL:       "boq_assistant-bard-web-server_20260525.09_p0",
		DefaultModel:   "gemini-3.6-flash",
		LogRequests:    true,
		CookieFile:     "",
		Proxy:          "",
		ProxyPoolURL:   "",
		ProxyMode:      "auto",
		Impersonate:    "chrome_146",
		DBPath:         "./data/gemini.db",
		AdminToken:     "",
		AdminEnabled:   true,
		RetentionDays:  30,
		// 实测：单 IP 累积 100+ 请求触发 sorry/index 拦截 → RPH=80 留 20% 余量。
		PerIPConcurrent: 5,
		PerIPRPM:        30,
		PerIPRPH:        80,
	}
}

func loadConfig(path string) error {
	cfg = defaultConfig()
	if path == "" {
		for _, p := range []string{"./config.json", os.ExpandEnv("$HOME/.config/gemini-web2api/config.json")} {
			if _, err := os.Stat(p); err == nil {
				path = p
				break
			}
		}
	}
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &cfg)
}
