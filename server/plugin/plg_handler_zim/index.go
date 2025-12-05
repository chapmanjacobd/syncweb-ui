package plg_handler_zim

import (
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	. "github.com/mickael-kerjean/filestash/server/common"
	"github.com/mickael-kerjean/filestash/server/ctrl"
	. "github.com/mickael-kerjean/filestash/server/middleware"
	"github.com/mickael-kerjean/filestash/server/model"
	. "github.com/mickael-kerjean/filestash/server/plugin/plg_backend_syncweb"
)

const (
	ZIM_URI          = "/zim"
	KIWIX_BIN        = "kiwix-serve"
	KIWIX_PORT_START = 8181
)

type OpdsEntry struct {
	Title string `xml:"title"`
	Name  string `xml:"name"`
	Link  []struct {
		Rel  string `xml:"rel,attr"`
		Href string `xml:"href,attr"`
		Type string `xml:"type,attr"`
	} `xml:"link"`
}

type OpdsFeed struct {
	XMLName xml.Name    `xml:"feed"`
	Entries []OpdsEntry `xml:"entry"`
}

type KiwixInstance struct {
	Process  *exec.Cmd
	Port     int
	ZimPath  string
	LastUsed time.Time
}

var (
	kiwixInstances = make(map[string]*KiwixInstance) // zimPath -> instance
	kiwixMutex     sync.Mutex
	usedPorts      = make(map[int]bool)
)

func init() {
	Hooks.Register.HttpEndpoint(func(r *mux.Router, app *App) error {
		// Handle viewing a specific .zim file
		r.HandleFunc(COOKIE_PATH+"zim/view", NewMiddlewareChain(
			ZimViewHandler,
			[]Middleware{SessionStart, LoggedInOnly},
			*app,
		)).Methods("GET")

		// Proxy to kiwix-serve instances
		r.PathPrefix(ZIM_URI + "/").HandlerFunc(ZimProxyHandler)

		return nil
	})

	// Register .zim files to open with the zim viewer
	Hooks.Register.XDGOpen(`
		if(location.pathname.toLowerCase().endsWith(".zim")) {
			return ["appframe", {"endpoint": "/api/zim/view"}];
		}
	`)

	// Cleanup old instances periodically
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			cleanupOldInstances()
		}
	}()
}

func ZimViewHandler(app *App, res http.ResponseWriter, req *http.Request) {
	if !model.CanRead(app) {
		SendErrorResult(res, ErrPermissionDenied)
		return
	}

	path := req.URL.Query().Get("path")
	if path == "" {
		SendErrorResult(res, NewError("Missing path parameter", http.StatusBadRequest))
		return
	}

	fullPath, err := ctrl.PathBuilder(app, path)
	if err != nil {
		SendErrorResult(res, err)
		return
	}

	// Verify the file exists and is a .zim file
	if !strings.HasSuffix(strings.ToLower(fullPath), ".zim") {
		SendErrorResult(res, NewError("Not a .zim file", http.StatusBadRequest))
		return
	}

	// Get the file from backend to verify it exists
	f, err := app.Backend.Cat(fullPath)
	if err != nil {
		SendErrorResult(res, err)
		return
	}
	f.Close()

	// Get or create kiwix instance for this file
	port, err := ensureKiwixServing(fullPath, app)
	if err != nil {
		SendErrorResult(res, err)
		return
	}

	// Wait a moment for kiwix to be ready
	time.Sleep(1 * time.Second)

	// Try to get the content URL from the catalog
	contentURL, err := getKiwixContentURL(port)
	if err != nil {
		Log.Warning("[zim] Could not parse catalog, using root URL: %s", err.Error())
		contentURL = ZIM_URI + "/"
	}

	// Generate iframe HTML
	zimName := filepath.Base(fullPath)
	html := fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
	<meta charset="utf-8">
	<meta name="viewport" content="width=device-width, initial-scale=1">
	<title>%s</title>
	<style>
		body, html {
			margin: 0;
			padding: 0;
			height: 100%%;
			overflow: hidden;
		}
		iframe {
			width: 100%%;
			height: 100%%;
			border: none;
		}
		.error {
			color: white;
			text-align: center;
			margin-top: 50px;
			font-size: 18px;
			opacity: 0.8;
			font-family: monospace;
		}
	</style>
</head>
<body>
	<iframe src="%s" allowfullscreen></iframe>
</body>
</html>`, zimName, contentURL)

	res.Header().Set("Content-Type", "text/html; charset=utf-8")
	res.Write([]byte(html))
}

func getKiwixContentURL(port int) (string, error) {
	catalogURL := fmt.Sprintf("http://127.0.0.1:%d%s/catalog/v2/entries", port, ZIM_URI)

	resp, err := http.Get(catalogURL)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("catalog returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var feed OpdsFeed
	if err := xml.Unmarshal(body, &feed); err != nil {
		return "", err
	}

	// If there's only one entry, find its content link
	if len(feed.Entries) == 1 {
		for _, link := range feed.Entries[0].Link {
			if link.Type == "text/html" {
				// Convert /zim/content/wikinews_en_all_maxi_2025-09
				// to /zim/viewer#wikinews_en_all_maxi_2025-09
				contentPath := strings.TrimPrefix(link.Href, ZIM_URI+"/content/")
				return fmt.Sprintf("http://127.0.0.1:%d%s/viewer#%s", port, ZIM_URI, contentPath), nil
			}
		}
	}

	// Multiple entries or no direct link found, use the root catalog
	return fmt.Sprintf("http://127.0.0.1:%d%s/viewer", port, ZIM_URI), nil
}

func ensureKiwixServing(zimPath string, app *App) (int, error) {
	kiwixMutex.Lock()
	defer kiwixMutex.Unlock()

	// Check if we already have an instance for this file
	if instance, exists := kiwixInstances[zimPath]; exists {
		instance.LastUsed = time.Now()
		return instance.Port, nil
	}

	// Get the actual file path
	localPath, err := getLocalZimPath(zimPath, app)
	if err != nil {
		return 0, err
	}

	// Find an available port
	port := findAvailablePort()
	if port == 0 {
		return 0, NewError("No available ports for kiwix-serve", http.StatusServiceUnavailable)
	}

	// Start kiwix-serve
	cmd := exec.Command(
		KIWIX_BIN,
		"-p", fmt.Sprintf("%d", port),
		"-r", ZIM_URI,
		localPath,
	)

	// Capture output for debugging
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		usedPorts[port] = false
		return 0, NewError(fmt.Sprintf("Failed to start kiwix-serve: %s", err.Error()), http.StatusInternalServerError)
	}

	// Store the instance
	kiwixInstances[zimPath] = &KiwixInstance{
		Process:  cmd,
		Port:     port,
		ZimPath:  zimPath,
		LastUsed: time.Now(),
	}
	usedPorts[port] = true

	Log.Info("[zim] started kiwix-serve on port %d for %s", port, zimPath)

	return port, nil
}

func findAvailablePort() int {
	// Try up to 100 ports starting from KIWIX_PORT_START
	for i := 0; i < 100; i++ {
		port := KIWIX_PORT_START + i
		if !usedPorts[port] {
			// Try to bind to the port to verify it's actually available
			if isPortAvailable(port) {
				return port
			}
		}
	}
	return 0
}

func isPortAvailable(port int) bool {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return false
	}
	listener.Close()
	return true
}

func cleanupOldInstances() {
	kiwixMutex.Lock()
	defer kiwixMutex.Unlock()

	// Clean up instances not used in the last 30 minutes
	cutoff := time.Now().Add(-30 * time.Minute)

	for zimPath, instance := range kiwixInstances {
		if instance.LastUsed.Before(cutoff) {
			Log.Info("[zim] cleaning up unused instance for %s on port %d", zimPath, instance.Port)

			if instance.Process != nil {
				if err := instance.Process.Process.Kill(); err != nil {
					Log.Warning("[zim] failed to stop kiwix-serve: %s", err.Error())
				}
				instance.Process.Wait()
			}

			usedPorts[instance.Port] = false
			delete(kiwixInstances, zimPath)
		}
	}
}

func getLocalZimPath(path string, app *App) (string, error) {
	backend := fmt.Sprintf("%T", app.Backend)

	if strings.HasSuffix(backend, "Local") {
		return path, nil
	}
	if strings.HasSuffix(backend, "Syncweb") {
		if syncwebBackend, ok := app.Backend.(*Syncweb); ok {
			localPath, err := syncwebBackend.ResolveLocalPath(path)
			if err != nil {
				return "", NewError(fmt.Sprintf("Could not resolve local path: %s", err.Error()), http.StatusNotImplemented)
			}
			return localPath, nil
		}
		return "", NewError("Failed to access Syncweb backend", http.StatusInternalServerError)
	}

	return "", NewError(fmt.Sprintf("Remote .zim files from %s not yet supported", backend), http.StatusNotImplemented)
}

func ZimProxyHandler(res http.ResponseWriter, req *http.Request) {
	// Extract the port from the request path or use a lookup
	// For now, we'll try to match based on the instance
	kiwixMutex.Lock()
	var targetPort int
	// Use the first available instance, or you could store port in session
	for _, instance := range kiwixInstances {
		targetPort = instance.Port
		instance.LastUsed = time.Now()
		break
	}
	kiwixMutex.Unlock()

	if targetPort == 0 {
		http.NotFound(res, req)
		return
	}

	req.URL.Path = strings.TrimPrefix(req.URL.Path, ZIM_URI)

	targetURL := fmt.Sprintf("http://127.0.0.1:%d", targetPort)

	u, err := url.Parse(targetURL)
	if err != nil {
		SendErrorResult(res, err)
		return
	}

	req.Header.Set("X-Forwarded-Host", req.Host+ZIM_URI)
	req.Header.Set("X-Forwarded-Proto", func() string {
		if scheme := req.Header.Get("X-Forwarded-Proto"); scheme != "" {
			return scheme
		} else if req.TLS != nil {
			return "https"
		}
		return "http"
	}())

	reverseProxy := &httputil.ReverseProxy{
		Director: func(rq *http.Request) {
			rq.URL.Scheme = u.Scheme
			rq.URL.Host = u.Host
			rq.URL.Path = func(a, b string) string {
				aslash := strings.HasSuffix(a, "/")
				bslash := strings.HasPrefix(b, "/")
				switch {
				case aslash && bslash:
					return a + b[1:]
				case !aslash && !bslash:
					return a + "/" + b
				}
				return a + b
			}(u.Path, rq.URL.Path)
			if u.RawQuery == "" || rq.URL.RawQuery == "" {
				rq.URL.RawQuery = u.RawQuery + rq.URL.RawQuery
			} else {
				rq.URL.RawQuery = u.RawQuery + "&" + rq.URL.RawQuery
			}
		},
	}
	reverseProxy.ErrorHandler = func(rw http.ResponseWriter, rq *http.Request, err error) {
		Log.Warning("[zim] proxy error: %s", err.Error())
		SendErrorResult(rw, NewError(err.Error(), http.StatusBadGateway))
	}
	reverseProxy.ServeHTTP(res, req)
}
