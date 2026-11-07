package app

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAdminUI_Redirect(t *testing.T) {
	req := httptest.NewRequest("GET", "/admin", nil)
	w := httptest.NewRecorder()
	handleAdminUI(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusMovedPermanently {
		t.Fatalf("expected 301, got %d", resp.StatusCode)
	}
	if loc := resp.Header.Get("Location"); loc != "admin/" {
		t.Fatalf("expected Location 'admin/', got %q", loc)
	}
}

func TestAdminUI_IndexHTML(t *testing.T) {
	req := httptest.NewRequest("GET", "/admin/", nil)
	w := httptest.NewRecorder()
	handleAdminUI(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	ct := resp.Header.Get("Content-Type")
	if !strings.HasPrefix(ct, "text/html") {
		t.Fatalf("expected text/html, got %q", ct)
	}

	body := w.Body.String()
	if !strings.Contains(body, `<div id="app"></div>`) {
		t.Fatalf("index.html missing #app root element")
	}

	// Verify no stray git conflict markers
	markers := []string{"<<<<<<<", "=======", ">>>>>>>"}
	for _, m := range markers {
		if strings.Contains(body, m) {
			t.Fatalf("index.html contains conflict marker: %s", m)
		}
	}
}

func TestAdminUI_StaticAsset(t *testing.T) {
	req := httptest.NewRequest("GET", "/admin/chart.umd.min.js", nil)
	w := httptest.NewRecorder()
	handleAdminUI(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "javascript") {
		t.Fatalf("expected javascript, got %q", ct)
	}
	if !bytes.Contains(w.Body.Bytes(), []byte("Chart")) {
		t.Fatalf("chart.umd.min.js missing Chart symbol")
	}
}

