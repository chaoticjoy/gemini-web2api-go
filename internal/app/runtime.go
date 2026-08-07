package app

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
)

// RuntimeConfig 是面板里能随时改、改完立刻生效的那部分配置。
//
// 部署期配置（监听地址、DB 路径、admin token、API key、cookie 文件）**不在这里**：
// 那些改了本来就要重启进程，放 docker-compose.yml 的 environment / command 更合适，
// 也避免把凭证放进一个网页表单。这里只放调优参数——为了改个超时重启一次服务太蠢。
//
// 取值优先级：面板改过的（存 kv 表） > config.json / CLI flag > 内置默认。
type RuntimeConfig struct {
	RetryAttempts   int    `json:"retry_attempts"`
	RetryDelaySec   int    `json:"retry_delay_sec"`
	RequestTimeout  int    `json:"request_timeout_sec"`
	DefaultModel    string `json:"default_model"`
	PerIPConcurrent int    `json:"per_ip_concurrent"`
	PerIPRPM        int    `json:"per_ip_rpm"`
	PerIPRPH        int    `json:"per_ip_rph"`
	RetentionDays   int    `json:"retention_days"`
	LogRequests     bool   `json:"log_requests"`
	Impersonate     string `json:"impersonate"`
	GeminiBL        string `json:"gemini_bl"`
	Proxy           string `json:"proxy"`
	ProxyPoolURL   string `json:"proxy_pool_url"`
	ProxyMode      string `json:"proxy_mode"`
}

const runtimeConfigKey = "runtime_config"

var (
	rtMu  sync.RWMutex
	rtVal RuntimeConfig
)

// initRuntimeConfig 用启动配置做基线，再把面板改过的值盖上去。
// 必须在 DB 打开之后调用。
func initRuntimeConfig() {
	base := RuntimeConfig{
		RetryAttempts:   cfg.RetryAttempts,
		RetryDelaySec:   cfg.RetryDelaySec,
		RequestTimeout:  cfg.RequestTimeout,
		DefaultModel:    cfg.DefaultModel,
		PerIPConcurrent: cfg.PerIPConcurrent,
		PerIPRPM:        cfg.PerIPRPM,
		PerIPRPH:        cfg.PerIPRPH,
		RetentionDays:   cfg.RetentionDays,
		LogRequests:     cfg.LogRequests,
		Impersonate:     cfg.Impersonate,
		GeminiBL:        cfg.GeminiBL,
		Proxy:           cfg.Proxy,
		ProxyPoolURL:   cfg.ProxyPoolURL,
		ProxyMode:      cfg.ProxyMode,
	}
	if base.ProxyMode == "" {
		base.ProxyMode = "auto"
	}
	if raw := kvGet(runtimeConfigKey); raw != "" {
		saved := base
		if err := json.Unmarshal([]byte(raw), &saved); err == nil {
			if err := validateRuntimeConfig(saved); err == nil {
				base = saved
			} else {
				logf("[config] 忽略 kv 里不合法的运行时配置: %v", err)
			}
		}
	}
	rtMu.Lock()
	rtVal = base
	rtMu.Unlock()
}

// rtCfg 返回当前运行时配置的快照。
func rtCfg() RuntimeConfig {
	rtMu.RLock()
	defer rtMu.RUnlock()
	return rtVal
}

// validateRuntimeConfig 校验面板传来的值。
//
// 这些数字直接决定重试次数、超时和限流额度，是外部输入进敏感落点：
// 0 或负数会让限流器永远拒绝或永远放行，超大值能把单个请求挂死几小时。
// 上界给得比任何合理用法都宽，只挡明显离谱的输入。
func validateRuntimeConfig(c RuntimeConfig) error {
	type rangeCheck struct {
		name     string
		v        int
		min, max int
	}
	for _, r := range []rangeCheck{
		{"retry_attempts", c.RetryAttempts, 1, 10},
		{"retry_delay_sec", c.RetryDelaySec, 0, 60},
		{"request_timeout_sec", c.RequestTimeout, 5, 600},
		{"per_ip_concurrent", c.PerIPConcurrent, 0, 1000},
		{"per_ip_rpm", c.PerIPRPM, 0, 10000},
		{"per_ip_rph", c.PerIPRPH, 0, 100000},
		{"retention_days", c.RetentionDays, 1, 3650},
	} {
		if r.v < r.min || r.v > r.max {
			return fmt.Errorf("%s=%d 超出允许范围 [%d, %d]", r.name, r.v, r.min, r.max)
		}
	}
	if _, _, err := resolveModel(c.DefaultModel); err != nil {
		return fmt.Errorf("default_model 不可用: %v", err)
	}
	if c.Impersonate == "" {
		return fmt.Errorf("impersonate 不能为空")
	}
	if c.GeminiBL == "" {
		return fmt.Errorf("gemini_bl 不能为空")
	}
	if c.ProxyPoolURL != "" {
		if !strings.HasPrefix(c.ProxyPoolURL, "http://") && !strings.HasPrefix(c.ProxyPoolURL, "https://") {
			return fmt.Errorf("proxy_pool_url 必须以 http:// 或 https:// 开头")
		}
	}
	if c.ProxyMode != "" && c.ProxyMode != "auto" && c.ProxyMode != "pool_only" && c.ProxyMode != "dynamic_only" && c.ProxyMode != "static_only" && c.ProxyMode != "direct_only" {
		return fmt.Errorf("proxy_mode 无效，支持的值: auto, pool_only, dynamic_only, static_only, direct_only")
	}
	return nil
}

// saveRuntimeConfig 校验并持久化，成功后立刻生效。
func saveRuntimeConfig(next RuntimeConfig) error {
	if err := validateRuntimeConfig(next); err != nil {
		return err
	}
	b, err := json.Marshal(next)
	if err != nil {
		return err
	}
	if err := kvSet(runtimeConfigKey, string(b)); err != nil {
		return err
	}
	rtMu.Lock()
	rtVal = next
	rtMu.Unlock()
	logf("[config] 运行时配置已更新")
	return nil
}
