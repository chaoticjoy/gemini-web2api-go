package app

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed admin_ui/*
var adminUIFS embed.FS

func handleAdminUI(w http.ResponseWriter, r *http.Request) {
	sub, err := fs.Sub(adminUIFS, "admin_ui")
	if err != nil {
		http.Error(w, "embed error", 500)
		return
	}
	// strip /admin/ prefix; "" -> index.html
	path := r.URL.Path
	if path == "/admin" || path == "/admin/" {
		path = "index.html"
	} else {
		path = path[len("/admin/"):]
	}
	f, err := sub.Open(path)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	stat, _ := f.Stat()
	data := make([]byte, stat.Size())
	f.Read(data)
	w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
	switch {
	case len(path) > 5 && path[len(path)-5:] == ".html":
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	case len(path) > 3 && path[len(path)-3:] == ".js":
		w.Header().Set("Content-Type", "application/javascript")
	case len(path) > 4 && path[len(path)-4:] == ".css":
		w.Header().Set("Content-Type", "text/css")
	default:
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	w.Write(data)
}
