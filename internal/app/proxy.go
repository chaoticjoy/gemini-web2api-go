package app

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Proxy struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	URL       string `json:"url"`
	Enabled   bool   `json:"enabled"`
	Weight    int    `json:"weight"`
	FailCount int    `json:"fail_count"`
	LastUsed  int64  `json:"last_used"`
	LastError string `json:"last_error"`
	CreatedAt int64  `json:"created_at"`
}

var (
	proxyMu     sync.RWMutex
	proxyCache  []Proxy
	proxyCursor uint64

	dynamicSlotsMu   sync.RWMutex
	dynamicProxyMap  = map[string]int64{}
	dynamicProxyURLs = map[int64]string{}
	nextDynamicID    int64 = -1000
)

func getDynamicSlotID(proxyURL string) int64 {
	dynamicSlotsMu.Lock()
	defer dynamicSlotsMu.Unlock()
	if id, ok := dynamicProxyMap[proxyURL]; ok {
		return id
	}
	id := nextDynamicID
	nextDynamicID--
	dynamicProxyMap[proxyURL] = id
	dynamicProxyURLs[id] = proxyURL
	return id
}

func isDynamicSlot(id int64) (string, bool) {
	dynamicSlotsMu.RLock()
	defer dynamicSlotsMu.RUnlock()
	url, ok := dynamicProxyURLs[id]
	return url, ok
}

// loadProxies refreshes the in-memory proxy list from DB.
func loadProxies() {
	rows, err := getDB().Query(`SELECT id, name, url, enabled, weight, fail_count,
        IFNULL(last_used,0), IFNULL(last_error,''), created_at FROM proxies ORDER BY id`)
	if err != nil {
		logf("[proxy] load failed: %v", err)
		return
	}
	defer rows.Close()
	var list []Proxy
	for rows.Next() {
		var p Proxy
		var enabled int
		if err := rows.Scan(&p.ID, &p.Name, &p.URL, &enabled, &p.Weight, &p.FailCount,
			&p.LastUsed, &p.LastError, &p.CreatedAt); err != nil {
			continue
		}
		p.Enabled = enabled == 1
		list = append(list, p)
	}
	proxyMu.Lock()
	proxyCache = list
	proxyMu.Unlock()
}

// pickProxyWithCapacity 找一个 enabled + fail<5 + 当前限流没满的代理。
// 返回 (proxy, ok)。所有代理都满时返回 ok=false。
//
// 跟旧的 pickProxy 区别：会问 trySlotAcquire 看 slot 是否有容量；
// 调用方拿到的 slot 必须配套调 slotRelease(proxy.ID)。
func pickProxyWithCapacity() (Proxy, bool) {
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	if len(proxyCache) == 0 {
		return Proxy{}, false
	}
	var pool []Proxy
	for _, p := range proxyCache {
		if p.Enabled && p.FailCount < 5 {
			pool = append(pool, p)
		}
	}
	if len(pool) == 0 {
		return Proxy{}, false
	}
	// 从轮询起点开始,找第一个 slot 没满的
	start := atomic.AddUint64(&proxyCursor, 1) - 1
	for i := 0; i < len(pool); i++ {
		p := pool[(int(start)+i)%len(pool)]
		if ok, _ := trySlotAcquire(p.ID); ok {
			return p, true
		}
	}
	return Proxy{}, false
}

func recordProxyResult(id int64, success bool, errStr string) {
	if id == 0 {
		return
	}
	if pURL, isDyn := isDynamicSlot(id); isDyn {
		reportRemoteProxyResult(rtCfg().ProxyPoolURL, pURL, success)
		return
	}
	now := time.Now().Unix()
	if success {
		_, _ = getDB().Exec(`UPDATE proxies SET fail_count=0, last_used=?, last_error='' WHERE id=?`, now, id)
	} else {
		_, _ = getDB().Exec(`UPDATE proxies SET fail_count=fail_count+1, last_used=?, last_error=? WHERE id=?`,
			now, errStr, id)
	}
	loadProxies()
}

// Dynamic Proxy Pool Client Functions ────────────────────────────────────────

type RemoteProxyItem struct {
	URL          string `json:"url"`
	Type         string `json:"type"`
	IP           string `json:"ip"`
	Port         string `json:"port"`
	SuccessCount int    `json:"success_count"`
	FailCount    int    `json:"fail_count"`
}

type RemoteProxyReportReq struct {
	URL     string `json:"url"`
	Success bool   `json:"success"`
}

func normalizePoolURL(poolURL string) string {
	u := strings.TrimSpace(poolURL)
	if u == "" {
		return ""
	}
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		u = "http://" + u
	}
	return strings.TrimRight(u, "/")
}

func fetchRemoteProxy(poolURL string) (string, error) {
	poolURL = normalizePoolURL(poolURL)
	if poolURL == "" {
		return "", errors.New("empty pool url")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	reqURL := poolURL + "/api/proxy"
	resp, err := client.Get(reqURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("remote pool HTTP %d", resp.StatusCode)
	}
	var item RemoteProxyItem
	if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
		return "", err
	}
	if item.URL == "" {
		return "", errors.New("empty proxy url from remote pool")
	}
	return item.URL, nil
}

func reportRemoteProxyResult(poolURL, proxyURL string, success bool) {
	poolURL = normalizePoolURL(poolURL)
	if poolURL == "" || proxyURL == "" {
		return
	}
	go func() {
		client := &http.Client{Timeout: 5 * time.Second}
		reqURL := strings.TrimRight(poolURL, "/") + "/api/proxy/report"
		payload := RemoteProxyReportReq{URL: proxyURL, Success: success}
		b, err := json.Marshal(payload)
		if err != nil {
			return
		}
		resp, err := client.Post(reqURL, "application/json", bytes.NewReader(b))
		if err == nil {
			_ = resp.Body.Close()
		}
	}()
}

func parseRemoteProxyItems(bodyBytes []byte) ([]RemoteProxyItem, error) {
	bodyBytes = bytes.TrimSpace(bodyBytes)
	if len(bodyBytes) == 0 {
		return nil, errors.New("empty response body")
	}

	// 1. 尝试 []RemoteProxyItem (JSON 对象数组)
	var rawItems []RemoteProxyItem
	if err := json.Unmarshal(bodyBytes, &rawItems); err == nil {
		var valid []RemoteProxyItem
		for _, item := range rawItems {
			if strings.TrimSpace(item.URL) != "" {
				valid = append(valid, item)
			}
		}
		if len(valid) > 0 {
			return valid, nil
		}
	}

	// 2. 尝试 []string (字符串数组)
	var strList []string
	if err := json.Unmarshal(bodyBytes, &strList); err == nil && len(strList) > 0 {
		var items []RemoteProxyItem
		for _, s := range strList {
			s = strings.TrimSpace(s)
			if s != "" {
				items = append(items, RemoteProxyItem{URL: s})
			}
		}
		if len(items) > 0 {
			return items, nil
		}
	}

	// 3. 尝试包裹对象 {"proxies": [...], ...}
	var rawMap map[string]json.RawMessage
	if err := json.Unmarshal(bodyBytes, &rawMap); err == nil {
		for _, k := range []string{"proxies", "items", "data", "list", "proxies_list"} {
			if sub, ok := rawMap[k]; ok {
				if parsed, err := parseRemoteProxyItems(sub); err == nil && len(parsed) > 0 {
					return parsed, nil
				}
			}
		}
	}

	// 4. 尝试按行分隔的文本
	lines := strings.Split(string(bodyBytes), "\n")
	var items []RemoteProxyItem
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && validateProxyURL(line) == nil {
			items = append(items, RemoteProxyItem{URL: line})
		}
	}
	if len(items) > 0 {
		return items, nil
	}

	return nil, errors.New("failed to parse proxy list (unsupported format)")
}

func syncRemoteProxies(poolURL string) (int, error) {
	poolURL = normalizePoolURL(poolURL)
	if poolURL == "" {
		return 0, errors.New("remote pool url is empty")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	reqURL := poolURL + "/api/proxy/all"
	resp, err := client.Get(reqURL)
	if err != nil {
		return 0, fmt.Errorf("failed to fetch proxies: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("remote pool HTTP %d", resp.StatusCode)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read response: %w", err)
	}

	items, err := parseRemoteProxyItems(bodyBytes)
	if err != nil {
		return 0, err
	}

	imported := 0
	for _, item := range items {
		pURL := strings.TrimSpace(item.URL)
		if pURL == "" {
			continue
		}
		if err := validateProxyURL(pURL); err != nil {
			continue
		}
		var count int
		err := getDB().QueryRow(`SELECT COUNT(1) FROM proxies WHERE url=?`, pURL).Scan(&count)
		if err != nil || count > 0 {
			continue
		}
		name := item.IP
		if item.Port != "" {
			name += ":" + item.Port
		}
		if name == "" {
			name = pURL
		}
		_, err = getDB().Exec(`INSERT INTO proxies(name, url, enabled, weight, created_at) VALUES (?,?,1,1,?)`,
			name, pURL, time.Now().Unix())
		if err == nil {
			imported++
		}
	}
	if imported > 0 {
		loadProxies()
	}
	return imported, nil
}

func testRemoteProxyPool(poolURL string) (map[string]interface{}, error) {
	poolURL = normalizePoolURL(poolURL)
	if poolURL == "" {
		return nil, errors.New("remote pool url is empty")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	reqURL := poolURL + "/api/proxy/stats"
	resp, err := client.Get(reqURL)
	if err != nil {
		reqURL = poolURL + "/health"
		resp2, err2 := client.Get(reqURL)
		if err2 != nil {
			return nil, fmt.Errorf("failed to connect to pool: %w", err)
		}
		defer resp2.Body.Close()
		return map[string]interface{}{"status": "ok", "message": "connected via /health"}, nil
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return map[string]interface{}{"status": "ok", "message": "connected"}, nil
	}
	return result, nil
}

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
	res, err := getDB().Exec(`INSERT INTO proxies(name, url, enabled, weight, created_at)
        VALUES (?,?,?,?,?)`, name, url, 1, weight, time.Now().Unix())
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	loadProxies()
	return id, nil
}

// validateProxyURL 校验代理 URL 协议（参考 Kiro-Gogogo 实现）。
// 支持 http / https / socks5 / socks5h。
func validateProxyURL(s string) error {
	if !strings.HasPrefix(s, "http://") &&
		!strings.HasPrefix(s, "https://") &&
		!strings.HasPrefix(s, "socks5://") &&
		!strings.HasPrefix(s, "socks5h://") {
		return errors.New("代理 URL 必须以 http:// / https:// / socks5:// / socks5h:// 开头")
	}
	return nil
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
