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
	"time"
)

// 动态代理 Slot 管理 ────────────────────────────────────────────────────────────

var (
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

// 动态代理统计与熔断冷却 ─────────────────────────────────────────────────────────

type dynamicProxyStat struct {
	failCount int
	lastUsed  int64
	lastError string
}

var (
	dynamicStatsMu sync.RWMutex
	dynamicStats   = map[string]*dynamicProxyStat{}
)

func recordDynamicProxyResult(proxyURL string, success bool, errStr string) {
	if proxyURL == "" {
		return
	}
	dynamicStatsMu.Lock()
	defer dynamicStatsMu.Unlock()

	now := time.Now().Unix()
	st, ok := dynamicStats[proxyURL]
	if !ok {
		st = &dynamicProxyStat{}
		dynamicStats[proxyURL] = st
	}
	st.lastUsed = now
	if success {
		st.failCount = 0
		st.lastError = ""
	} else {
		st.failCount++
		st.lastError = errStr
		cooldownMin := rtCfg().ProxyCooldownMin
		threshold := proxyFailThreshold()
		if st.failCount == threshold && cooldownMin > 0 {
			logf("[proxy-dynamic] 动态代理 %s 连续失败达到重试上限（%d 次），进入熔断冷却（%d 分钟）", proxyURL, threshold, cooldownMin)
		}
	}

	if len(dynamicStats) > 500 {
		cooldownSec := int64(rtCfg().ProxyCooldownMin) * 60
		if cooldownSec <= 0 {
			cooldownSec = 7200
		}
		for k, v := range dynamicStats {
			if now-v.lastUsed > cooldownSec*2 && v.failCount == 0 {
				delete(dynamicStats, k)
			}
		}
	}
}

// forceDynamicProxyCooldown 在动态代理发生不可用错误（如连接拒绝、EOF、建匿名会话失败）或切代理时直接进入熔断冷却
func forceDynamicProxyCooldown(proxyURL string, errStr string) {
	if proxyURL == "" {
		return
	}
	dynamicStatsMu.Lock()
	defer dynamicStatsMu.Unlock()

	now := time.Now().Unix()
	st, ok := dynamicStats[proxyURL]
	if !ok {
		st = &dynamicProxyStat{}
		dynamicStats[proxyURL] = st
	}
	st.lastUsed = now
	st.failCount = proxyFailThreshold()
	st.lastError = errStr
	cooldownMin := rtCfg().ProxyCooldownMin
	if cooldownMin > 0 {
		logf("[proxy-dynamic] 动态代理 %s 发生连接故障，直接进入熔断冷却（%d 分钟）: %s", proxyURL, cooldownMin, errStr)
	}
}

// isDynamicProxyCooling 判断动态代理是否处于冷却期
func isDynamicProxyCooling(proxyURL string) bool {
	if proxyURL == "" {
		return false
	}
	cooldownMin := rtCfg().ProxyCooldownMin
	if cooldownMin <= 0 {
		return false
	}
	dynamicStatsMu.RLock()
	st, ok := dynamicStats[proxyURL]
	dynamicStatsMu.RUnlock()
	if !ok || st == nil {
		return false
	}
	if st.failCount < proxyFailThreshold() {
		return false
	}
	now := time.Now().Unix()
	return now-st.lastUsed < int64(cooldownMin)*60
}

// 粘性代理管理 ─────────────────────────────────────────────────────────────────

var (
	stickyMu       sync.RWMutex
	lastStickyURL  string
	lastStickyID   int64
	lastStickyName string
)

func getStickyProxy() (string, int64, string) {
	stickyMu.RLock()
	defer stickyMu.RUnlock()
	return lastStickyURL, lastStickyID, lastStickyName
}

func setStickyProxy(url string, id int64, name string) {
	stickyMu.Lock()
	lastStickyURL = url
	lastStickyID = id
	lastStickyName = name
	stickyMu.Unlock()
}

func clearStickyProxy() {
	stickyMu.Lock()
	lastStickyURL = ""
	lastStickyID = 0
	lastStickyName = ""
	stickyMu.Unlock()
}

func fetchOrStickyRemoteProxy(poolURL string) (Proxy, bool) {
	return fetchOrStickyRemoteProxyExcept(poolURL, nil)
}

func fetchOrStickyRemoteProxyExcept(poolURL string, excludedURLs map[string]bool) (Proxy, bool) {
	if rtCfg().ProxyStrategy == "sticky" {
		sURL, sID, sName := getStickyProxy()
		if _, isDyn := isDynamicSlot(sID); sURL != "" && isDyn {
			if (excludedURLs != nil && excludedURLs[sURL]) || isDynamicProxyCooling(sURL) {
				clearStickyProxy()
			} else if ok, _ := trySlotAcquire(sID); ok {
				return Proxy{ID: sID, Name: sName, URL: sURL, Enabled: true}, true
			}
		}
	}
	const maxFetchAttempts = 10
	for fetchAttempt := 0; fetchAttempt < maxFetchAttempts; fetchAttempt++ {
		pURL, err := fetchRemoteProxy(poolURL)
		if err != nil {
			logf("[proxy-pool] 从动态代理池 (%s) 获取代理失败: %v", poolURL, err)
			return Proxy{}, false
		}
		if pURL == "" {
			return Proxy{}, false
		}
		if (excludedURLs != nil && excludedURLs[pURL]) || isDynamicProxyCooling(pURL) {
			continue
		}
		slotID := getDynamicSlotID(pURL)
		if ok, _ := trySlotAcquire(slotID); ok {
			pName := "Dynamic (" + pURL + ")"
			if rtCfg().ProxyStrategy == "sticky" {
				setStickyProxy(pURL, slotID, pName)
			}
			return Proxy{
				ID:      slotID,
				Name:    pName,
				URL:     pURL,
				Enabled: true,
			}, true
		}
	}
	return Proxy{}, false
}

// 远程代理池客户端接口 ─────────────────────────────────────────────────────────────

type RemoteProxyItem struct {
	URL  string `json:"url"`
	Type string `json:"type"`
	IP   string `json:"ip"`
	Port string `json:"port"`
}

type RemoteProxyReportReq struct {
	URL     string `json:"url"`
	Success bool   `json:"success"`
}

func normalizePoolURL(raw string) string {
	s := strings.TrimSpace(raw)
	s = strings.TrimRight(s, "/")
	return s
}

func fetchRemoteProxy(poolURL string) (string, error) {
	poolURL = normalizePoolURL(poolURL)
	if poolURL == "" {
		return "", errors.New("remote pool url is empty")
	}
	client := &http.Client{Timeout: 5 * time.Second}
	reqURL := poolURL + "/api/proxy"
	resp, err := client.Get(reqURL)
	if err != nil {
		return "", fmt.Errorf("fetch remote proxy failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("remote pool HTTP %d", resp.StatusCode)
	}
	var item RemoteProxyItem
	if err := json.NewDecoder(resp.Body).Decode(&item); err != nil {
		return "", fmt.Errorf("decode remote proxy item failed: %w", err)
	}
	pURL := strings.TrimSpace(item.URL)
	if pURL == "" {
		return "", errors.New("remote pool returned empty proxy url")
	}
	if err := validateProxyURL(pURL); err != nil {
		return "", fmt.Errorf("invalid proxy url returned from pool: %w", err)
	}
	return pURL, nil
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

