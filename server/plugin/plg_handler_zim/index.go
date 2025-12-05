package plg_handler_zim

import (
	"encoding/xml"
	"fmt"
	"io"
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

const ZIM_URI = "/zim"

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

var (
	plugin_enable  func() bool
	kiwix_port     func() int
	kiwix_path     func() string
	kiwixProcess   *exec.Cmd
	kiwixMutex     sync.Mutex
	activeZimFiles = make(map[string]bool)
	zimFilesMutex  sync.RWMutex
)

func init() {
	plugin_enable = func() bool {
		return Config.Get("features.zim.enable").Schema(func(f *FormElement) *FormElement {
			if f == nil {
				f = &FormElement{}
			}
			f.Name = "enable"
			f.Type = "enable"
			f.Target = []string{"zim_kiwix_path", "zim_port"}
			f.Description = "Enable/Disable support for viewing .zim files using kiwix-serve"
			f.Default = false
			if p := os.Getenv("KIWIX_PATH"); p != "" {
				f.Default = true
			}
			return f
		}).Bool()
	}

	kiwix_path = func() string {
		return Config.Get("features.zim.kiwix_path").Schema(func(f *FormElement) *FormElement {
			if f == nil {
				f = &FormElement{}
			}
			f.Id = "zim_kiwix_path"
			f.Name = "kiwix_path"
			f.Type = "text"
			f.Description = "Path to kiwix-serve executable"
			f.Default = "kiwix-serve"
			f.Placeholder = "Eg: /usr/local/bin/kiwix-serve"
			if p := os.Getenv("KIWIX_PATH"); p != "" {
				f.Default = p
				f.Placeholder = fmt.Sprintf("Default: '%s'", p)
			}
			return f
		}).String()
	}

	kiwix_port = func() int {
		return Config.Get("features.zim.port").Schema(func(f *FormElement) *FormElement {
			if f == nil {
				f = &FormElement{}
			}
			f.Id = "zim_port"
			f.Name = "port"
			f.Type = "number"
			f.Description = "Port for kiwix-serve to listen on"
			f.Default = 8181
			f.Placeholder = "Default: 8181"
			return f
		}).Int()
	}

	Hooks.Register.Onload(func() {
		plugin_enable()
		kiwix_path()
		kiwix_port()
	})

	Hooks.Register.HttpEndpoint(func(r *mux.Router, app *App) error {
		if !plugin_enable() {
			return nil
		}

		// Handle viewing a specific .zim file
		r.HandleFunc(COOKIE_PATH+"zim/view", NewMiddlewareChain(
			ZimViewHandler,
			[]Middleware{SessionStart, LoggedInOnly},
			*app,
		)).Methods("GET")

		// Proxy to kiwix-serve
		r.PathPrefix(ZIM_URI + "/").HandlerFunc(ZimProxyHandler)

		return nil
	})

	// Register .zim files to open with the zim viewer
	Hooks.Register.XDGOpen(`
		if(location.pathname.toLowerCase().endsWith(".zim")) {
			return ["appframe", {"endpoint": "/api/zim/view"}];
		}
	`)
}

func ZimViewHandler(app *App, res http.ResponseWriter, req *http.Request) {
	if !plugin_enable() {
		Log.Warning("plg_handler_zim::handler request_disabled")
		return
	}

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

	// Start or restart kiwix-serve with this file
	if err := ensureKiwixServing(fullPath, app); err != nil {
		SendErrorResult(res, err)
		return
	}

	// Wait a moment for kiwix to be ready
	time.Sleep(1 * time.Second)

	// Direct link (without kiwix-server header: random button and searchbox)
	// contentURL, err := getKiwixContentURL()
	// if err != nil {
	// 	Log.Warning("[zim] Could not parse catalog, using root URL: %s", err.Error())
	// 	contentURL = ZIM_URI + "/"
	// }
	port := kiwix_port()
	contentURL := fmt.Sprintf("http://127.0.0.1:%d%s", port, ZIM_URI)

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

func getKiwixContentURL() (string, error) {
	port := kiwix_port()
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
				url := fmt.Sprintf("http://127.0.0.1:%d%s", port, link.Href)
				return url, nil
			}
		}
	}

	// Multiple entries or no direct link found, use the root catalog
	url := fmt.Sprintf("http://127.0.0.1:%d%s", port, ZIM_URI)
	return url, nil
}

func ensureKiwixServing(zimPath string, app *App) error {
	kiwixMutex.Lock()
	defer kiwixMutex.Unlock()

	// Check if we need to start/restart kiwix-serve
	zimFilesMutex.RLock()
	isServing := activeZimFiles[zimPath]
	zimFilesMutex.RUnlock()

	if isServing && kiwixProcess != nil {
		// Already serving this file
		return nil
	}

	// Stop existing kiwix-serve if running
	if kiwixProcess != nil {
		if err := kiwixProcess.Process.Kill(); err != nil {
			Log.Warning("[zim] failed to stop existing kiwix-serve: %s", err.Error())
		}
		kiwixProcess.Wait()
		kiwixProcess = nil
	}

	// Get the actual file path (may need to download from backend)
	localPath, err := getLocalZimPath(zimPath, app)
	if err != nil {
		return NewError(fmt.Sprintf("Failed to access .zim file: %s", err.Error()), http.StatusInternalServerError)
	}

	// Start kiwix-serve
	kiwixBin := kiwix_path()
	port := kiwix_port()

	cmd := exec.Command(
		kiwixBin,
		"-p", fmt.Sprintf("%d", port),
		"-r", ZIM_URI,
		localPath,
	)

	// Capture output for debugging
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return NewError(fmt.Sprintf("Failed to start kiwix-serve: %s", err.Error()), http.StatusInternalServerError)
	}

	kiwixProcess = cmd

	// Mark this file as being served
	zimFilesMutex.Lock()
	activeZimFiles = map[string]bool{zimPath: true}
	zimFilesMutex.Unlock()

	// Wait a moment for kiwix-serve to start
	time.Sleep(500 * time.Millisecond)

	Log.Info("[zim] started kiwix-serve on port %d for %s", port, zimPath)

	return nil
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
	if !plugin_enable() {
		http.NotFound(res, req)
		return
	}

	req.URL.Path = strings.TrimPrefix(req.URL.Path, ZIM_URI)

	port := kiwix_port()
	targetURL := fmt.Sprintf("http://127.0.0.1:%d", port)

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
