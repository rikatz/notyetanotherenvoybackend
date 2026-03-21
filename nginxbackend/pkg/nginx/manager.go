package nginx

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	crossplane "github.com/nginxinc/nginx-go-crossplane"
)

// Manager handles NGINX configuration generation and process management
type Manager struct {
	configPath  string
	nginxBinary string
	cmd         *exec.Cmd
}

// NewManager creates a new NGINX manager
func NewManager(configPath, nginxBinary string) (*Manager, error) {
	if configPath == "" {
		configPath = "/etc/nginx"
	}
	if nginxBinary == "" {
		nginxBinary = "/usr/sbin/nginx"
	}

	// Create config directory if it doesn't exist
	if err := os.MkdirAll(configPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create config directory: %w", err)
	}

	// Create subdirectories for different config types
	for _, subdir := range []string{"endpoints", "servers", "routes", "certs"} {
		subdirPath := filepath.Join(configPath, subdir)
		if err := os.MkdirAll(subdirPath, 0755); err != nil {
			return nil, fmt.Errorf("failed to create %s directory: %w", subdir, err)
		}
	}

	// Create /tmp/nginx directory for PID file
	if err := os.MkdirAll("/tmp/nginx", 0755); err != nil {
		return nil, fmt.Errorf("failed to create /tmp/nginx directory: %w", err)
	}

	return &Manager{
		configPath:  configPath,
		nginxBinary: nginxBinary,
	}, nil
}

// Start starts the NGINX process in the background
func (m *Manager) Start(ctx context.Context) error {
	log := slog.With("nginx", m.nginxBinary)

	// First test the configuration
	testCmd := exec.Command(m.nginxBinary, "-t")
	if output, err := testCmd.CombinedOutput(); err != nil {
		log.Error("nginx configuration test failed", "error", err, "output", string(output))
		return fmt.Errorf("nginx config test failed: %w (output: %s)", err, string(output))
	}
	log.Info("nginx configuration test passed")

	// Start NGINX with context
	m.cmd = exec.CommandContext(ctx, m.nginxBinary, "-g", "daemon off;")
	m.cmd.Stdout = os.Stdout
	m.cmd.Stderr = os.Stderr

	if err := m.cmd.Start(); err != nil {
		log.Error("failed to start nginx", "error", err)
		return fmt.Errorf("failed to start nginx: %w", err)
	}

	log.Info("nginx process started", "pid", m.cmd.Process.Pid)

	// Wait a moment to ensure NGINX started successfully
	time.Sleep(100 * time.Millisecond)

	// Check if process is still running
	if m.cmd.ProcessState != nil && m.cmd.ProcessState.Exited() {
		return fmt.Errorf("nginx process exited immediately")
	}

	// Start a goroutine to wait for the process and log when it exits
	go func() {
		err := m.cmd.Wait()
		if err != nil {
			log.Error("nginx process exited with error", "error", err)
			panic(err)
		} else {
			log.Info("nginx process exited")
		}
	}()

	return nil
}

// WriteConfig writes an upstream configuration block to a file
func (m *Manager) WriteConfig(filename string, config *crossplane.Config) error {
	fullPath := filepath.Join(m.configPath, filename)
	log := slog.With("file", fullPath)

	// Use crossplane to build the configuration
	var buf bytes.Buffer
	if err := crossplane.Build(&buf, *config, &crossplane.BuildOptions{}); err != nil {
		log.Error("failed to build nginx config", "error", err)
		return fmt.Errorf("failed to build nginx config: %w", err)
	}

	if err := os.WriteFile(fullPath, buf.Bytes(), 0644); err != nil {
		log.Error("failed to write config file", "error", err)
		return fmt.Errorf("failed to write config file: %w", err)
	}

	log.Info("nginx configuration written")
	return nil
}

// DeleteConfig removes a configuration file
func (m *Manager) DeleteConfig(filename string) error {
	fullPath := filepath.Join(m.configPath, filename)
	log := slog.With("file", fullPath)

	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			log.Debug("config file does not exist, nothing to delete")
			return nil
		}
		log.Error("failed to delete config file", "error", err)
		return fmt.Errorf("failed to delete config file: %w", err)
	}

	log.Info("nginx configuration deleted")
	return nil
}

// Reload tests and reloads the NGINX configuration
func (m *Manager) Reload() error {
	log := slog.With("nginx", m.nginxBinary)

	// First test the configuration
	testCmd := exec.Command(m.nginxBinary, "-t")
	if output, err := testCmd.CombinedOutput(); err != nil {
		log.Error("nginx configuration test failed", "error", err, "output", string(output))
		return fmt.Errorf("nginx config test failed: %w (output: %s)", err, string(output))
	}
	log.Debug("nginx configuration test passed")

	// Reload NGINX
	reloadCmd := exec.Command(m.nginxBinary, "-s", "reload")
	if output, err := reloadCmd.CombinedOutput(); err != nil {
		log.Error("nginx reload failed", "error", err, "output", string(output))
		return fmt.Errorf("nginx reload failed: %w (output: %s)", err, string(output))
	}

	log.Info("nginx reloaded successfully")
	return nil
}

// ServiceKeyToFilename converts a service key (namespace/name/port) to a filesystem-safe filename
func ServiceKeyToFilename(serviceKey string) string {
	// Replace / with - for filesystem safety
	// Format: namespace/name/port -> namespace-name-port.conf
	return strings.ReplaceAll(serviceKey, "/", "-") + ".conf"
}

// ServiceKeyToUpstreamName converts a service key to an NGINX upstream name
func ServiceKeyToUpstreamName(serviceKey string) string {
	// Replace / with _ for NGINX identifier safety
	// Format: namespace/name/port -> namespace_name_port
	return strings.ReplaceAll(serviceKey, "/", "_")
}

// ConfigPath returns the base configuration path
func (m *Manager) ConfigPath() string {
	return m.configPath
}
