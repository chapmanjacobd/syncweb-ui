package plg_backend_syncweb

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"syscall"
	"time"

	. "github.com/mickael-kerjean/filestash/server/common"
)

var home = func() string {
	if homeDir, err := os.UserHomeDir(); err == nil && homeDir != "" {
		return homeDir
	}
	if u, err := user.Current(); err == nil && u.HomeDir != "" {
		return u.HomeDir
	}
	return "."
}()

var syncwebConfigPath = appConfigPath("syncweb")

type SyncthingConfigXML struct {
	XMLName xml.Name `xml:"configuration"`
	GUI     struct {
		Address string `xml:"address"`
		APIKey  string `xml:"apikey"`
	} `xml:"gui"`
}

func (s Syncweb) loadConfigFromXML() (*SyncthingConfigXML, error) {
	data, err := os.ReadFile(syncwebConfigPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file at %s: %w", syncwebConfigPath, err)
	}

	var stConfig SyncthingConfigXML
	if err := xml.Unmarshal(data, &stConfig); err != nil {
		return nil, fmt.Errorf("failed to parse XML config from %s: %w", syncwebConfigPath, err)
	}

	if stConfig.GUI.APIKey == "" {
		return nil, NewError("API key field was empty in the XML configuration", 500)
	}
	if stConfig.GUI.Address == "" {
		return nil, NewError("GUI address field was empty in the XML configuration", 500)
	}

	return &stConfig, nil
}

func (s Syncweb) request(method string, path string, body io.Reader) (*http.Response, error) {
	url := strings.TrimSuffix(s.URL, "/") + path

	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	req.Header.Set("X-API-Key", s.APIKey)
	if method == "POST" && strings.HasPrefix(path, "/rest/config/ignore") {
		req.Header.Set("Content-Type", "text/plain")
	} else if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	return HTTPClient.Do(req)
}

// API /rest/config/folder
type StFolder struct {
	ID   string `json:"id"`
	Path string `json:"path"`
}

// API /rest/db/browse
type StFile struct {
	Name    string      `json:"name"`
	Type    string      `json:"type"`
	Size    int64       `json:"size"`
	ModTime RFC3339Nano `json:"modTime"`
}

func (s Syncweb) ResolveLocalPath(path string) (string, error) {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 {
		return "", NewError("Invalid path", 400)
	}

	folderID := parts[0]
	// Path must have at least the folder ID
	if localRootPath, exists := s.FoldersMap[folderID]; exists {
		// Handle path being just "/FolderID"
		if len(parts) == 1 {
			return localRootPath, nil
		}

		relativePath := strings.Join(parts[1:], "/")
		return filepath.Join(localRootPath, relativePath), nil
	}

	return "", NewError("Folder ID not found while resolving local path: "+folderID, 404)
}

func (s Syncweb) folders() ([]StFolder, error) {
	resp, err := s.request("GET", "/rest/config/folders", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to fetch folders: %s", resp.Status)
	}

	var folders []StFolder
	if err := json.NewDecoder(resp.Body).Decode(&folders); err != nil {
		return nil, err
	}
	return folders, nil
}

func (s Syncweb) files(folderID string, prefix string) ([]StFile, error) {
	apiPath := fmt.Sprintf("/rest/db/browse?folder=%s&prefix=%s", folderID, prefix)

	resp, err := s.request("GET", apiPath, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("failed to browse path %s: %s", prefix, resp.Status)
	}

	var stFiles []StFile
	if err := json.NewDecoder(resp.Body).Decode(&stFiles); err != nil {
		return nil, err
	}
	return stFiles, nil
}

func init() {
	Backend.Register("syncweb", &Syncweb{})
}

type Syncweb struct {
	URL        string
	APIKey     string
	FoldersMap map[string]string // folderID -> local folder root path
}

func (s *Syncweb) Init(params map[string]string, app *App) (IBackend, error) {
	backend := &Syncweb{
		FoldersMap: make(map[string]string),
	}

	if env := os.Getenv("SYNCWEB_URL"); env != "" {
		backend.URL = env
	} else {
		backend.URL = Config.Get("backend.syncweb.url").Default("http://127.0.0.1:8384").String()
	}

	if env := os.Getenv("SYNCWEB_API_KEY"); env != "" {
		backend.APIKey = env
	} else {
		backend.APIKey = Config.Get("backend.syncweb.api_key").Default("").String()
	}

	// When ENV VARS not defined, try loading from config.xml
	if backend.APIKey == "" || strings.Contains(backend.URL, "127.0.0.1:8384") {
		xmlConfig, err := s.loadConfigFromXML()
		if err == nil {
			if backend.URL == "http://127.0.0.1:8384" && xmlConfig.GUI.Address != "" {
				backend.URL = "http://" + xmlConfig.GUI.Address
			}
			if backend.APIKey == "" && xmlConfig.GUI.APIKey != "" {
				backend.APIKey = xmlConfig.GUI.APIKey
			}
		} else {
			fmt.Printf("Warning: Failed to read Syncthing config from XML: %v\n", err)
		}
	}

	if backend.APIKey == "" {
		return backend, NewError("Missing API Key: Syncthing API Key is required.", 502)
	}

	// Populate FoldersMap
	folders, err := backend.folders()
	if err != nil {
		return backend, NewError("Failed to connect to Syncthing API: "+err.Error(), 502)
	}
	for _, f := range folders {
		backend.FoldersMap[f.ID] = f.Path
	}

	return backend, nil
}

func (s Syncweb) LoginForm() Form {
	return Form{
		Elmnts: []FormElement{
			{
				Name:  "type",
				Type:  "hidden",
				Value: "syncweb",
			},
		},
	}
}

func (s Syncweb) Ls(path string) ([]os.FileInfo, error) {
	path = filepath.Clean(path)
	files := make([]os.FileInfo, 0)

	// Case 1: Root path "/" - List all configured folders
	if path == "/" {
		for folderID, localPath := range s.FoldersMap {
			// Check local existence of the root folder path itself
			_, err := os.Lstat(localPath)
			isLocal := err == nil

			files = append(files, File{
				FName:   folderID,
				FPath:   localPath,
				FType:   "directory",
				FTime:   time.Now().Unix(),
				FSize:   0,
				Offline: !isLocal,
			})
		}
		return files, nil
	}

	// Case 2: Sub-directory path "/<folderID>/sub/path"
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 {
		return nil, NewError("Invalid path", 400)
	}

	folderID := parts[0]
	relativePath := strings.Join(parts[1:], "/")
	localRoot, exists := s.FoldersMap[folderID]
	if !exists {
		return nil, NewError("Folder ID not found: "+folderID, 404)
	}

	stFiles, err := s.files(folderID, relativePath)
	if err != nil {
		return nil, NewError("Syncthing API error during browse: "+err.Error(), 500)
	}

	for _, stFile := range stFiles {
		// fmt.Print(stFile.Type)
		fType := func(s string) string {
			switch s {
			case "FILE_INFO_TYPE_DIRECTORY":
				return "directory"
			case "FILE_INFO_TYPE_FILE":
				return "file"
			case "FILE_INFO_TYPE_SYMLINK", "FILE_INFO_TYPE_SYMLINK_FILE", "FILE_INFO_TYPE_SYMLINK_DIRECTORY":
				return "symlink"
			}
			return s
		}(stFile.Type)

		files = append(files, File{
			FName: stFile.Name,
			FPath: filepath.Join(localRoot, relativePath, stFile.Name),
			FType: fType,
			FTime: stFile.ModTime.Unix(),
			FSize: stFile.Size,
		})
	}

	return files, nil
}

func (s Syncweb) Cat(path string) (io.ReadCloser, error) {
	// returns the local file reader if available, or triggers synchronization if remote
	path = filepath.Clean(path)

	localPath, err := s.ResolveLocalPath(path)
	if err != nil {
		// This path does not resolve to a local Syncthing folder
		return nil, err
	}

	fs, statErr := os.Lstat(localPath)
	if fs.IsDir() {
		return nil, ErrNotFound
	} else if statErr == nil {
		// File exists locally, return it directly via file system
		return os.OpenFile(localPath, os.O_RDONLY|syscall.O_NOFOLLOW, os.ModePerm)
	}

	// If it doesn't exist, trigger synchronization via .stignore
	// /<FolderID>/relative/path/to/file.txt
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 {
		return nil, NewError("File not found locally and path is too short to trigger remote sync.", 404)
	}

	folderID := parts[0]
	relativePath := strings.Join(parts[1:], "/")

	// Get current .stignore content
	currentIgnore, err := s.request("GET", "/rest/config/ignore?folder="+folderID, nil)
	if err != nil {
		return nil, NewError("Failed to read .stignore: "+err.Error(), 500)
	}
	defer currentIgnore.Body.Close()

	ignoreBytes, err := io.ReadAll(currentIgnore.Body)
	if err != nil {
		return nil, NewError("Failed to read .stignore response body: "+err.Error(), 500)
	}

	marker := "!" + relativePath
	newIgnoreContent := string(ignoreBytes)

	// Check if the marker is already present
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(marker) + `\s*$`)
	if re.MatchString(newIgnoreContent) {
		// File is already marked for sync (un-ignored)
		return nil, NewError("File '"+relativePath+"' is already marked for synchronization. Check Syncthing status", 202) // 202 Accepted
	}

	// Append the new marker on a new line
	if newIgnoreContent != "" && !strings.HasSuffix(newIgnoreContent, "\n") {
		newIgnoreContent += "\n"
	}
	newIgnoreContent += marker + "\n"

	// POST the updated content back
	newIgnoreReader := strings.NewReader(newIgnoreContent)
	postResponse, err := s.request("POST", "/rest/config/ignore?folder="+folderID, newIgnoreReader)
	if err != nil {
		return nil, NewError("Failed to update .stignore via API: "+err.Error(), 500)
	}
	defer postResponse.Body.Close()

	if postResponse.StatusCode != 200 {
		return nil, NewError(HTTPFriendlyStatus(postResponse.StatusCode)+": failed to update .stignore", postResponse.StatusCode)
	}
	// Return 202 Accepted status for async download trigger
	return nil, NewError("Downloading '"+relativePath+"'", 202)
}

func (s Syncweb) Rm(path string) error {
	path = filepath.Clean(path)

	localPath, err := s.ResolveLocalPath(path)
	if err != nil {
		return NewError("Cannot resolve path for deletion: "+err.Error(), 400)
	}

	_, statErr := os.Lstat(localPath)
	if os.IsNotExist(statErr) {
		return NewError("File not found locally. Deletion not supported for remote Syncthing files.", 405)
	} else if statErr != nil {
		return NewError("Filesystem access error: "+statErr.Error(), 500)
	}

	// TODO: delete Syncweb folder when deleting folder root

	// Exists locally, perform deletion
	return os.RemoveAll(localPath)
}

func (s Syncweb) Mv(from string, to string) error {
	localFromPath, err := s.ResolveLocalPath(from)
	if err != nil {
		return NewError("Cannot resolve source path for move: "+err.Error(), 400)
	}
	localToPath, err := s.ResolveLocalPath(to)
	if err != nil {
		return NewError("Cannot resolve destination path for move: "+err.Error(), 400)
	}

	// Check local existence of the source file
	_, statErr := os.Lstat(localFromPath)
	if os.IsNotExist(statErr) {
		return NewError("File not found locally. Movement not supported for remote Syncthing files.", 405)
	}
	if statErr != nil {
		return NewError("Filesystem access error: "+statErr.Error(), 500)
	}

	// File exists locally, perform move (rename)
	if err := os.Rename(localFromPath, localToPath); err != nil {
		return NewError("Failed to move local file: "+err.Error(), 500)
	}
	return nil
}

func (s Syncweb) Save(path string, content io.Reader) error {
	path = filepath.Clean(path)

	localPath, err := s.ResolveLocalPath(path)
	if err != nil {
		return NewError("Cannot resolve path for save: "+err.Error(), 400)
	}

	f, err := os.OpenFile(localPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC|syscall.O_NOFOLLOW, 0o664)
	if err != nil {
		return err
	}
	_, err = io.Copy(f, content)
	return err
}

func (s Syncweb) Touch(path string) error {
	path = filepath.Clean(path)

	localPath, err := s.ResolveLocalPath(path)
	if err != nil {
		return NewError("Cannot resolve path for save: "+err.Error(), 400)
	}

	// differs from Save() by not strictly issuing trunc syscall
	f, err := os.OpenFile(localPath, os.O_WRONLY|os.O_CREATE|syscall.O_NOFOLLOW, 0o664)
	if err != nil {
		return err
	}
	if _, err = f.Write([]byte("")); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (s Syncweb) syncwebDefaultFolderRoot() string {
	return filepath.Join(home, "Syncweb")
}

func (s Syncweb) Mkdir(path string) error {
	path = filepath.Clean(path)
	parent := filepath.Dir(path)

	// Case 1: Parent path is "/" - Make new Syncweb folder
	if parent == "/" {
		path = filepath.Join(s.syncwebDefaultFolderRoot(), path)
		// Because folderID != FName, the UI could show that no folder with a CertainName exists
		// while concurrently ~/Syncweb/CertainName already exists--just with a different folderID

		// This situation is confusing and we will at the very least need to raise an error message
		// when attempting to create a Syncweb folder where one already exists
		if _, err := os.Lstat(path); err == nil {
			return NewError(fmt.Sprintf("'%s' already exists", path), 400)
		}

		folderID, err := s.createFolderID(path)
		if err != nil {
			return err
		}

		// Build folder definition
		data := map[string]any{
			"id":    folderID,
			"label": filepath.Base(path),
			"path":  path,
			"type":  "sendonly",
		}

		// Merge default folder config
		def, err := s.defaultFolder()
		if err != nil {
			return err
		}
		for k, v := range def {
			if _, exists := data[k]; !exists {
				data[k] = v
			}
		}

		if err := s.addFolder(data); err != nil {
			return err
		}
		if err := s.setIgnores(folderID, []string{}); err != nil {
			return err
		}

	} else {
		// Case 2: Sub-directory path "/<folderID>/sub/path"
		parts := strings.Split(strings.Trim(path, "/"), "/")
		if len(parts) == 0 {
			return NewError("Invalid path", 400)
		}

		folderID := parts[0]
		localRoot, exists := s.FoldersMap[folderID]
		if !exists {
			return NewError("Folder ID not found: "+folderID, 404)
		}
		relativePath := strings.Join(parts[1:], "/")
		path = filepath.Join(localRoot, relativePath)
	}

	return os.MkdirAll(path, 0o755)
}

func (s Syncweb) folderStats() (map[string]any, error) {
	resp, err := s.request("GET", "/rest/stats/folder", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func (s Syncweb) createFolderID(path string) (string, error) {
	stats, err := s.folderStats()
	if err != nil {
		return "", err
	}

	name := filepath.Base(path)
	if _, exists := stats[name]; !exists {
		return name, nil
	}

	return pathHash(path), nil
}

func pathHash(path string) string {
	abs, _ := filepath.Abs(path)
	sum := sha1.Sum([]byte(abs))
	encoded := base64.RawURLEncoding.EncodeToString(sum[:])
	return encoded
}

func (s Syncweb) addFolder(data map[string]any) error {
	body, _ := json.Marshal(data)
	resp, err := s.request("POST", "/rest/config/folders", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (s Syncweb) setIgnores(folderID string, lines []string) error {
	body, _ := json.Marshal(map[string]any{"ignore": lines})
	resp, err := s.request("POST", "/rest/db/ignores?folder="+url.QueryEscape(folderID), bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (s Syncweb) defaultFolder() (map[string]any, error) {
	resp, err := s.request("GET", "/rest/config/defaults/folder", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var m map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

func appConfigPath(appName string) string {
	if home == "." {
		// if user home cannot be detected
		return filepath.Join(home, "config.xml")
	}

	switch runtime.GOOS {
	case "windows":
		appData := os.Getenv("LOCALAPPDATA")
		if appData == "" {
			appData = filepath.Join(home, "AppData", "Local")
		}
		return filepath.Join(appData, appName, "config.xml")
	case "darwin":
		return filepath.Join(home, "Library", "Application Support", appName, "config.xml")
	default:
		// Linux/other: $XDG_STATE_HOME/appname or ~/.local/state/appname
		xdgState := os.Getenv("XDG_STATE_HOME")
		if xdgState != "" {
			return filepath.Join(xdgState, appName, "config.xml")
		} else {
			return filepath.Join(home, ".local", "state", appName, "config.xml")
		}
	}
}

type RFC3339Nano struct{ time.Time }

func (ut *RFC3339Nano) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}

	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return err
	}

	*ut = RFC3339Nano{Time: t}

	return nil
}
