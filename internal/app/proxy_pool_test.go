package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
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
