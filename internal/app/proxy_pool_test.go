package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDynamicProxyPoolClient(t *testing.T) {
	var reported int32

	// 创建 Mock 动态代理池服务器
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/proxy":
			_ = json.NewEncoder(w).Encode(RemoteProxyItem{
				URL:  "socks5://127.0.0.1:1080",
				Type: "socks5",
				IP:   "127.0.0.1",
				Port: "1080",
			})
		case "/api/proxy/all":
			_ = json.NewEncoder(w).Encode([]RemoteProxyItem{
				{URL: "http://1.1.1.1:8080", Type: "http", IP: "1.1.1.1", Port: "8080"},
				{URL: "socks5://2.2.2.2:1080", Type: "socks5", IP: "2.2.2.2", Port: "1080"},
			})
		case "/api/proxy/report":
			var req RemoteProxyReportReq
			_ = json.NewDecoder(r.Body).Decode(&req)
			if req.URL != "" {
				atomic.AddInt32(&reported, 1)
			}
			w.WriteHeader(http.StatusOK)
		case "/api/proxy/stats":
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"total": 2,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	// 1. 测试 fetchRemoteProxy
	pURL, err := fetchRemoteProxy(ts.URL)
	if err != nil {
		t.Fatalf("fetchRemoteProxy failed: %v", err)
	}
	if pURL != "socks5://127.0.0.1:1080" {
		t.Fatalf("unexpected proxy URL: %s", pURL)
	}

	// 2. 测试 getDynamicSlotID & isDynamicSlot
	slotID := getDynamicSlotID(pURL)
	if slotID >= 0 {
		t.Fatalf("expected negative slotID for dynamic proxy, got %d", slotID)
	}
	u, ok := isDynamicSlot(slotID)
	if !ok || u != pURL {
		t.Fatalf("isDynamicSlot returned (%s, %v), expected (%s, true)", u, ok, pURL)
	}

	// 3. 测试 testRemoteProxyPool
	stats, err := testRemoteProxyPool(ts.URL)
	if err != nil {
		t.Fatalf("testRemoteProxyPool failed: %v", err)
	}
	if stats["total"] != float64(2) {
		t.Fatalf("unexpected stats: %v", stats)
	}

	// 4. 测试 reportRemoteProxyResult
	reportRemoteProxyResult(ts.URL, pURL, true)
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt32(&reported) == 0 {
		t.Fatalf("expected reportRemoteProxyResult to be called")
	}
}

func TestParseRemoteProxyItems(t *testing.T) {
	// 测试多种格式解析
	jsonArr := []byte(`[{"url":"socks5://1.1.1.1:1080"},{"url":"http://2.2.2.2:8080"}]`)
	items, err := parseRemoteProxyItems(jsonArr)
	if err != nil || len(items) != 2 {
		t.Fatalf("failed jsonArr parse: %v, %v", err, items)
	}

	strArr := []byte(`["socks5://1.1.1.1:1080", "http://2.2.2.2:8080"]`)
	items, err = parseRemoteProxyItems(strArr)
	if err != nil || len(items) != 2 {
		t.Fatalf("failed strArr parse: %v, %v", err, items)
	}

	objWrap := []byte(`{"proxies":["socks5://1.1.1.1:1080", "http://2.2.2.2:8080"]}`)
	items, err = parseRemoteProxyItems(objWrap)
	if err != nil || len(items) != 2 {
		t.Fatalf("failed objWrap parse: %v, %v", err, items)
	}

	textLines := []byte("socks5://1.1.1.1:1080\nhttp://2.2.2.2:8080")
	items, err = parseRemoteProxyItems(textLines)
	if err != nil || len(items) != 2 {
		t.Fatalf("failed textLines parse: %v, %v", err, items)
	}
}

func TestProxyModeAcquireSlot(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(RemoteProxyItem{
			URL:  "socks5://127.0.0.1:1080",
			Type: "socks5",
		})
	}))
	defer ts.Close()

	// 1. direct_only
	rtMu.Lock()
	rtVal.ProxyMode = "direct_only"
	rtVal.ProxyPoolURL = ts.URL
	rtMu.Unlock()
	p, ok, err := acquireSlot(0)
	if !ok || err != nil || p.ID != 0 {
		t.Fatalf("direct_only failed: p=%+v, ok=%v, err=%v", p, ok, err)
	}
	releaseSlot(p.ID)

	// 2. dynamic_only
	rtMu.Lock()
	rtVal.ProxyMode = "dynamic_only"
	rtMu.Unlock()
	p, ok, err = acquireSlot(0)
	if !ok || err != nil || p.ID >= 0 || p.URL != "socks5://127.0.0.1:1080" {
		t.Fatalf("dynamic_only failed: p=%+v, ok=%v, err=%v", p, ok, err)
	}
	releaseSlot(p.ID)

	// 3. sticky vs round_robin
	rtMu.Lock()
	rtVal.ProxyStrategy = "sticky"
	rtMu.Unlock()
	pSticky, okSticky := fetchOrStickyRemoteProxy(ts.URL)
	if !okSticky || pSticky.URL != "socks5://127.0.0.1:1080" {
		t.Fatalf("sticky fetch failed: %+v", pSticky)
	}
	releaseSlot(pSticky.ID)

	// reset to auto
	rtMu.Lock()
	rtVal.ProxyMode = "auto"
	rtVal.ProxyStrategy = "sticky"
	rtVal.ProxyPoolURL = ""
	rtMu.Unlock()
}

func TestProxyRetryAttemptsConfig(t *testing.T) {
	cfg := RuntimeConfig{
		RetryAttempts:  3,
		RetryDelaySec:  2,
		RequestTimeout: 180,
		RetentionDays:  30,
		DefaultModel:   "gemini-3.6-flash",
		Impersonate:    "chrome_146",
		GeminiBL:       "test_bl",
	}

	// 0 到 10 应合法
	cfg.ProxyRetryAttempts = 0
	if err := validateRuntimeConfig(cfg); err != nil {
		t.Fatalf("expected 0 to be valid, got %v", err)
	}
	cfg.ProxyRetryAttempts = 10
	if err := validateRuntimeConfig(cfg); err != nil {
		t.Fatalf("expected 10 to be valid, got %v", err)
	}

	// 超出范围应报错
	cfg.ProxyRetryAttempts = -1
	if err := validateRuntimeConfig(cfg); err == nil {
		t.Fatalf("expected -1 to be invalid")
	}
	cfg.ProxyRetryAttempts = 11
	if err := validateRuntimeConfig(cfg); err == nil {
		t.Fatalf("expected 11 to be invalid")
	}
}

func TestAcquireSlotExceptWithExclusions(t *testing.T) {
	urls := []string{
		"socks5://127.0.0.1:1080",
		"socks5://127.0.0.1:1081",
	}
	idx := 0
	var mu sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		u := urls[idx%len(urls)]
		idx++
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(RemoteProxyItem{
			URL:  u,
			Type: "socks5",
		})
	}))
	defer ts.Close()

	rtMu.Lock()
	rtVal.ProxyMode = "dynamic_only"
	rtVal.ProxyPoolURL = ts.URL
	rtVal.ProxyStrategy = "round_robin"
	rtMu.Unlock()
	defer func() {
		rtMu.Lock()
		rtVal.ProxyMode = "auto"
		rtVal.ProxyStrategy = "sticky"
		rtVal.ProxyPoolURL = ""
		rtMu.Unlock()
	}()

	// 1. 无排除时，正常获取
	p1, ok1, err1 := acquireSlotExcept(0, nil, nil)
	if !ok1 || err1 != nil {
		t.Fatalf("acquireSlotExcept without exclusions failed: %v", err1)
	}
	releaseSlot(p1.ID)

	// 2. 排除 p1.URL，下一次应该拿到另一个 URL
	excludedURLs := map[string]bool{p1.URL: true}
	p2, ok2, err2 := acquireSlotExcept(0, nil, excludedURLs)
	if !ok2 || err2 != nil {
		t.Fatalf("acquireSlotExcept with exclusion failed: %v", err2)
	}
	defer releaseSlot(p2.ID)

	if p2.URL == p1.URL {
		t.Fatalf("expected different proxy URL, but got %s again", p2.URL)
	}
}

func TestPickProxyPreferringExceptStatic(t *testing.T) {
	proxyMu.Lock()
	oldCache := proxyCache
	now := time.Now().Unix()
	proxyCache = []Proxy{
		{ID: 101, Name: "Proxy-1", URL: "socks5://1.1.1.1:1080", Enabled: true, LastUsed: now},
		{ID: 102, Name: "Proxy-2", URL: "socks5://2.2.2.2:1080", Enabled: true, LastUsed: now},
	}
	proxyMu.Unlock()
	defer func() {
		proxyMu.Lock()
		proxyCache = oldCache
		proxyMu.Unlock()
	}()

	// 排除 101 时优先选 102
	p, ok := pickProxyPreferringExcept(101, map[int64]bool{101: true})
	if !ok || p.ID != 102 {
		t.Fatalf("expected proxy 102, got p=%+v, ok=%v", p, ok)
	}
	releaseSlot(p.ID)

	// 排除 101 和 102 时无可用
	_, okAll := pickProxyPreferringExcept(0, map[int64]bool{101: true, 102: true})
	if okAll {
		t.Fatalf("expected no proxy available when all are excluded")
	}
}

func TestDynamicProxyCooldown(t *testing.T) {
	u1 := "socks5://10.0.0.1:1080"
	u2 := "socks5://10.0.0.2:1080"

	// 确保重置状态
	dynamicStatsMu.Lock()
	delete(dynamicStats, u1)
	delete(dynamicStats, u2)
	dynamicStatsMu.Unlock()
	defer func() {
		dynamicStatsMu.Lock()
		delete(dynamicStats, u1)
		delete(dynamicStats, u2)
		dynamicStatsMu.Unlock()
	}()

	rtMu.Lock()
	oldCooldown := rtVal.ProxyCooldownMin
	oldRetry := rtVal.RetryAttempts
	rtVal.ProxyCooldownMin = 120
	rtVal.RetryAttempts = 3
	rtMu.Unlock()
	defer func() {
		rtMu.Lock()
		rtVal.ProxyCooldownMin = oldCooldown
		rtVal.RetryAttempts = oldRetry
		rtMu.Unlock()
	}()

	// 1. 初始状态：未冷却
	if isDynamicProxyCooling(u1) {
		t.Fatalf("expected u1 not cooling initially")
	}

	// 2. 失败 2 次：未达到重试次数 3
	for i := 0; i < 2; i++ {
		recordDynamicProxyResult(u1, false, "timeout")
	}
	if isDynamicProxyCooling(u1) {
		t.Fatalf("expected u1 not cooling after 2 failures (threshold=3)")
	}

	// 3. 失败第 3 次：达到重试次数，进入冷却
	recordDynamicProxyResult(u1, false, "connection refused")
	if !isDynamicProxyCooling(u1) {
		t.Fatalf("expected u1 to be in cooling after 3 failures (threshold=3)")
	}

	// 4. Mock 动态代理池返回 u1 和 u2，测试 fetchOrStickyRemoteProxyExcept 自动跳过冷却中的 u1
	urls := []string{u1, u2}
	idx := 0
	var mu sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		u := urls[idx%len(urls)]
		idx++
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(RemoteProxyItem{
			URL:  u,
			Type: "socks5",
		})
	}))
	defer ts.Close()

	rtMu.Lock()
	rtVal.ProxyMode = "dynamic_only"
	rtVal.ProxyPoolURL = ts.URL
	rtVal.ProxyStrategy = "round_robin"
	rtMu.Unlock()
	defer func() {
		rtMu.Lock()
		rtVal.ProxyMode = "auto"
		rtVal.ProxyStrategy = "sticky"
		rtVal.ProxyPoolURL = ""
		rtMu.Unlock()
	}()

	// 因为 u1 在冷却中，即使代理池轮询返回 u1，fetchOrStickyRemoteProxyExcept 也应该自动跳过 u1 并拿到 u2
	p, ok := fetchOrStickyRemoteProxyExcept(ts.URL, nil)
	if !ok {
		t.Fatalf("fetchOrStickyRemoteProxyExcept failed to find usable proxy")
	}
	defer releaseSlot(p.ID)
	if p.URL != u2 {
		t.Fatalf("expected proxy %s (since u1 is in cooling), got %s", u2, p.URL)
	}

	// 5. 成功后恢复：recordDynamicProxyResult 成功清零
	recordDynamicProxyResult(u1, true, "")
	if isDynamicProxyCooling(u1) {
		t.Fatalf("expected u1 not cooling after success")
	}

	// 6. proxy_cooldown_min = 0 时禁用冷却
	recordDynamicProxyResult(u1, false, "err")
	for i := 0; i < 5; i++ {
		recordDynamicProxyResult(u1, false, "err")
	}
	rtMu.Lock()
	rtVal.ProxyCooldownMin = 0
	rtMu.Unlock()
	if isDynamicProxyCooling(u1) {
		t.Fatalf("expected u1 not cooling when ProxyCooldownMin == 0")
	}
}


