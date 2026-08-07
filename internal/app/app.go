package app

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"time"
)

func logf(format string, args ...interface{}) {
	if rtCfg().LogRequests {
		fmt.Fprintf(os.Stderr, "[%s] %s\n",
			time.Now().Format("15:04:05"),
			fmt.Sprintf(format, args...))
	}
}

// maskKey shows "sk-gemini-XXXX...YYYY" for banner. Empty stays empty.
func maskKey(k string) string {
	if k == "" {
		return "(disabled)"
	}
	if len(k) <= 12 {
		return "****"
	}
	return k[:12] + "..." + k[len(k)-4:]
}

// Run 是进程入口，由根目录的 main.go 调用。
func Run() {
	port := flag.Int("port", 0, "listening port (default: 8083 or config.json)")
	configPath := flag.String("config", "", "path to config.json")
	cookieFile := flag.String("cookie-file", "", "path to cookie file")
	proxy := flag.String("proxy", "", "HTTP proxy fallback when proxy pool is empty")
	impersonate := flag.String("impersonate", "", "TLS profile (chrome_146, chrome_133, firefox_147, ...)")
	dbPath := flag.String("db", "", "SQLite path (default: ./data/gemini.db)")
	adminToken := flag.String("admin-token", "", "admin token (empty = no auth, only safe on 127.0.0.1)")
	apiKey := flag.String("api-key", "", "API key for /v1/* (locked). Empty = use kv-table key (auto-gen on first boot, mutable from admin UI)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("gemini-web2api-go %s\n", Version)
		return
	}

	if err := loadConfig(*configPath); err != nil {
		fmt.Fprintf(os.Stderr, "config load error: %v\n", err)
		os.Exit(1)
	}
	if *port > 0 {
		cfg.Port = *port
	}
	if *cookieFile != "" {
		cfg.CookieFile = *cookieFile
	}
	if *proxy != "" {
		cfg.Proxy = *proxy
	}
	if *impersonate != "" {
		cfg.Impersonate = *impersonate
	}
	if *dbPath != "" {
		cfg.DBPath = *dbPath
	}
	if *adminToken != "" {
		cfg.AdminToken = *adminToken
	}
	if envTok := os.Getenv("ADMIN_TOKEN"); envTok != "" && cfg.AdminToken == "" {
		cfg.AdminToken = envTok
	}

	// boot DB and load proxy pool
	getDB()
	initRuntimeConfig() // 面板改过的运行时配置盖在启动配置之上
	initCookie()        // 面板存的 cookie 优先于 --cookie-file
	loadProxies()
	resolvedAPIKey := initAPIKey(*apiKey)
	initTokenizer()
	startScheduler()

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/models", requireAPIKey(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "OPTIONS":
			handleOptions(w, r)
		case "GET":
			handleModels(w, r)
		default:
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
		}
	}))
	mux.HandleFunc("/v1/chat/completions", requireAPIKey(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "OPTIONS":
			handleOptions(w, r)
		case "POST":
			handleChatCompletions(w, r)
		default:
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
		}
	}))
	mux.HandleFunc("/v1/responses", requireAPIKey(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "OPTIONS":
			handleOptions(w, r)
		case "POST":
			handleResponses(w, r)
		default:
			writeJSON(w, 405, map[string]string{"error": "method not allowed"})
		}
	}))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			handleRoot(w, r)
		} else {
			writeJSON(w, 404, map[string]string{"error": "not found"})
		}
	})

	// ─── Admin UI + API ────────────────────────────────────────────────
	if cfg.AdminEnabled {
		mux.HandleFunc("/admin", handleAdminUI)
		mux.HandleFunc("/admin/", handleAdminUI)
		mux.HandleFunc("/admin/api/login", handleAdminLogin)
		mux.HandleFunc("/admin/api/logout", handleAdminLogout)
		mux.HandleFunc("/admin/api/stats", requireAuth(handleAdminStats))
		mux.HandleFunc("/admin/api/timeseries", requireAuth(handleAdminTimeseries))
		mux.HandleFunc("/admin/api/requests", requireAuth(handleAdminRequests))
		mux.HandleFunc("/admin/api/proxies", requireAuth(handleAdminProxies))
		mux.HandleFunc("/admin/api/proxies/sync", requireAuth(handleAdminProxySync))
		mux.HandleFunc("/admin/api/proxies/test-pool", requireAuth(handleAdminProxyTestPool))
		mux.HandleFunc("/admin/api/proxies/", requireAuth(handleAdminProxyItem))
		mux.HandleFunc("/admin/api/apikey", requireAuth(handleAdminAPIKey))
		mux.HandleFunc("/admin/api/usage", requireAuth(handleAdminUsage))
		mux.HandleFunc("/admin/api/config", requireAuth(handleAdminConfig))
		mux.HandleFunc("/admin/api/cookie", requireAuth(handleAdminCookie))
		mux.HandleFunc("/admin/api/cookies", requireAuth(handleAdminCookies))
		mux.HandleFunc("/admin/api/cookies/", requireAuth(handleAdminCookieItem))
		mux.HandleFunc("/admin/api/test", requireAuth(handleAdminTest))
	}

	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	server := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0, // streaming responses can be long
	}

	cookieStatus := "none (anonymous)"
	if hasCookie() {
		cookieStatus = "yes"
		if cfg.CookieFile != "" && kvGet("google_cookie") == "" {
			cookieStatus += " (" + cfg.CookieFile + ")"
		} else {
			cookieStatus += " (admin panel)"
		}
	}
	proxyStatus := rtCfg().Proxy
	if proxyStatus == "" {
		proxyStatus = "none"
	}
	var modelNames []string
	for n := range availableModels() {
		modelNames = append(modelNames, n)
	}
	sort.Strings(modelNames)

	fmt.Printf("gemini-web2api-go v%s\n", Version)
	fmt.Printf("  Listening:   http://%s\n", addr)
	fmt.Printf("  Base URL:    http://localhost:%d/v1\n", cfg.Port)
	fmt.Printf("  API key:     %s%s\n", maskKey(resolvedAPIKey),
		map[bool]string{true: "  (locked by flag/env)", false: "  (mutable in admin UI)"}[apiKeyLocked])
	if cfg.AdminEnabled {
		authMode := "no auth (open)"
		if cfg.AdminToken != "" {
			authMode = "token auth"
		}
		fmt.Printf("  Admin UI:    http://localhost:%d/admin  (%s)\n", cfg.Port, authMode)
	}
	fmt.Printf("  DB:          %s\n", cfg.DBPath)
	fmt.Printf("  Models:      %v\n", modelNames)
	fmt.Printf("  Cookie:      %s\n", cookieStatus)
	fmt.Printf("  Proxy:       %s\n", proxyStatus)
	fmt.Printf("  Impersonate: %s\n", rtCfg().Impersonate)
	tokInfo := "chars/4 (fallback)"
	if tokenizerOK {
		tokInfo = "tiktoken cl100k_base"
	}
	fmt.Printf("  Tokenizer:   %s\n", tokInfo)
	fmt.Printf("  Per-IP 限流: 并发=%d / RPM=%d / RPH=%d\n",
		rtCfg().PerIPConcurrent, rtCfg().PerIPRPM, rtCfg().PerIPRPH)
	fmt.Printf("  Retry:       %dx / %ds\n", rtCfg().RetryAttempts, rtCfg().RetryDelaySec)
	fmt.Println()

	if err := server.ListenAndServe(); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
