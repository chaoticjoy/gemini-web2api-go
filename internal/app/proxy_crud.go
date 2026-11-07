package app

import (
	"errors"
	"strings"
	"time"
)

// CRUD ───────────────────────────────────────────────────────────────────────

func proxyCreate(name, url string, weight int) (int64, error) {
	if name == "" || url == "" {
		return 0, errors.New("name and url required")
	}
	if err := validateProxyURL(url); err != nil {
		return 0, err
	}
	if weight <= 0 {
		weight = 1
	}
	id, err := insertID(`INSERT INTO proxies(name, url, enabled, weight, created_at)
        VALUES (?,?,?,?,?)`, name, url, 1, weight, time.Now().Unix())
	if err != nil {
		return 0, err
	}
	loadProxies()
	return id, nil
}

// validateProxyURL 校验代理 URL 协议。
// 支持 http / https / socks5 / socks5h。
//
// scheme 按大小写不敏感比：URL 的 scheme 本来就不区分大小写，url.Parse 会统一转小写，
// 所以 HTTP:// 在 4.0.0 那条不做校验的静态代理路径上是能正常工作的。校验若大小写
// 敏感，升级时就会把这种值拦下来 —— 用户什么都没改，代理却不工作了。
func validateProxyURL(s string) error {
	low := strings.ToLower(s)
	for _, p := range []string{"http://", "https://", "socks5://", "socks5h://"} {
		if strings.HasPrefix(low, p) {
			return nil
		}
	}
	return errors.New("代理 URL 必须以 http:// / https:// / socks5:// / socks5h:// 开头")
}

func proxyUpdate(id int64, name, url string, enabled *bool, weight *int) error {
	q := `UPDATE proxies SET `
	args := []interface{}{}
	parts := []string{}
	if name != "" {
		parts = append(parts, "name=?")
		args = append(args, name)
	}
	if url != "" {
		parts = append(parts, "url=?")
		args = append(args, url)
	}
	if enabled != nil {
		v := 0
		if *enabled {
			v = 1
		}
		parts = append(parts, "enabled=?")
		args = append(args, v)
	}
	if weight != nil {
		parts = append(parts, "weight=?")
		args = append(args, *weight)
	}
	if len(parts) == 0 {
		return nil
	}
	q += joinComma(parts) + " WHERE id=?"
	args = append(args, id)
	_, err := getDB().Exec(q, args...)
	if err == nil {
		loadProxies()
	}
	return err
}

func proxyDelete(id int64) error {
	_, err := getDB().Exec(`DELETE FROM proxies WHERE id=?`, id)
	if err == nil {
		loadProxies()
	}
	return err
}

func proxyResetFailures(id int64) error {
	_, err := getDB().Exec(`UPDATE proxies SET fail_count=0, last_error='' WHERE id=?`, id)
	if err == nil {
		loadProxies()
	}
	return err
}

func listProxies() []Proxy {
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	out := make([]Proxy, len(proxyCache))
	copy(out, proxyCache)
	return out
}

func joinComma(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ", "
		}
		out += p
	}
	return out
}

// proxyNameByID 按 id 找代理名，找不到返回空串（0 = 还没绑 / 直连）。
func proxyNameByID(id int64) string {
	if id <= 0 {
		return ""
	}
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	for _, p := range proxyCache {
		if p.ID == id {
			return p.Name
		}
	}
	return ""
}

// proxyUsableByID 这个出口现在还能不能用（存在 + enabled + 没熔断或已过冷却）。
// 判断账号绑定的出口是不是还有效，无效才该重新绑。
func proxyUsableByID(id int64) bool {
	if id <= 0 {
		return false
	}
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	now := time.Now().Unix()
	cooldown := rtCfg().ProxyCooldownMin
	for _, p := range proxyCache {
		if p.ID == id {
			return proxyUsable(p, now, cooldown)
		}
	}
	return false
}

// proxyURLByID 按 id 取代理 URL，找不到返回空串（直连）。
func proxyURLByID(id int64) string {
	if id <= 0 {
		return ""
	}
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	for _, p := range proxyCache {
		if p.ID == id {
			return p.URL
		}
	}
	return ""
}
