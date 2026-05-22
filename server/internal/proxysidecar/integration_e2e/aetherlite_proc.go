// Aetherlite subprocess helpers shared by harness.go (regular .go) and
// aetherlite_proc_test.go (TestMain). Lives in a non-_test file so the
// harness can reference `getAetherlite` / `waitForTCP` — Go forbids
// regular files from importing symbols defined in `_test.go` siblings.
//
// Build tag `e2e` mirrors the harness/test gating; nothing outside the
// e2e flow needs this code.

//go:build e2e

package integration_e2e

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// aetherliteStartTimeout caps how long we wait for aetherlite to start
// listening on its gRPC port after launch.
const aetherliteStartTimeout = 30 * time.Second

// sharedAetherlite is the per-package real-gateway process, lazily
// started by the first getAetherlite call and torn down by TestMain.
var (
	sharedAetherlite *aetherliteProc
	aetherliteOnce   sync.Once
	aetherliteErr    error
)

type aetherliteProc struct {
	cmd      *exec.Cmd
	grpcAddr string // 127.0.0.1:<port>
	opsAddr  string // 127.0.0.1:<port> for Prometheus /metrics
	dataDir  string
}

// getAetherlite returns the shared aetherlite process address,
// starting it on first call. Fails the test fast on startup error.
func getAetherlite(t *testing.T) *aetherliteProc {
	t.Helper()
	aetherliteOnce.Do(func() {
		proc, err := startAetherlite()
		if err != nil {
			aetherliteErr = err
			return
		}
		sharedAetherlite = proc
	})
	if aetherliteErr != nil {
		t.Fatalf("e2e: aetherlite startup failed: %v", aetherliteErr)
	}
	if sharedAetherlite == nil {
		t.Fatalf("e2e: aetherlite not started")
	}
	return sharedAetherlite
}

func startAetherlite() (*aetherliteProc, error) {
	binPath, err := buildAetherliteBin()
	if err != nil {
		return nil, fmt.Errorf("build aetherlite: %w", err)
	}
	grpcPort, err := pickFreeTCPPort()
	if err != nil {
		return nil, fmt.Errorf("pick grpc port: %w", err)
	}
	adminPort, err := pickFreeTCPPort()
	if err != nil {
		return nil, fmt.Errorf("pick admin port: %w", err)
	}
	opsPort, err := pickFreeTCPPort()
	if err != nil {
		return nil, fmt.Errorf("pick ops port: %w", err)
	}
	dataDir, err := os.MkdirTemp("", "aetherlite-e2e-")
	if err != nil {
		return nil, fmt.Errorf("mkdir data: %w", err)
	}
	cmd := exec.Command(binPath,
		"-port", fmt.Sprintf("%d", grpcPort),
		"-admin-port", fmt.Sprintf("%d", adminPort),
		"-ops-port", fmt.Sprintf("%d", opsPort),
		"-data-dir", dataDir,
		"-dev",
		"-insecure-admin",
		"-workflow=false",
	)
	// Enable audit logging with tight flush settings so e2e tests can observe
	// audit events without waiting the default 5-second flush period.
	cmd.Env = append(os.Environ(),
		"AETHER_AUDIT_ENABLED=true",
		"AETHER_AUDIT_BATCH_SIZE=1",
		"AETHER_AUDIT_FLUSH_PERIOD=200ms",
	)
	// Stream aetherlite output to stderr so failures surface in -v runs.
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = os.RemoveAll(dataDir)
		return nil, fmt.Errorf("start: %w", err)
	}
	grpcAddr := fmt.Sprintf("127.0.0.1:%d", grpcPort)
	opsAddr := fmt.Sprintf("127.0.0.1:%d", opsPort)
	if err := waitForTCP(grpcAddr, aetherliteStartTimeout); err != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = os.RemoveAll(dataDir)
		return nil, fmt.Errorf("wait ready %s: %w", grpcAddr, err)
	}
	return &aetherliteProc{cmd: cmd, grpcAddr: grpcAddr, opsAddr: opsAddr, dataDir: dataDir}, nil
}

func (a *aetherliteProc) stop() {
	if a == nil || a.cmd == nil || a.cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-a.cmd.Process.Pid, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_ = a.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = syscall.Kill(-a.cmd.Process.Pid, syscall.SIGKILL)
		<-done
	}
	_ = os.RemoveAll(a.dataDir)
}

// buildAetherliteBin compiles aetherlite into a cache directory; uses
// Go's build cache so repeat runs are near-instant.
func buildAetherliteBin() (string, error) {
	moduleRoot, err := findModuleRoot()
	if err != nil {
		return "", err
	}
	cacheDir := filepath.Join(os.TempDir(), "aether-e2e-bin")
	if err := os.MkdirAll(cacheDir, 0o755); err != nil {
		return "", err
	}
	binPath := filepath.Join(cacheDir, "aetherlite")
	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/aetherlite")
	cmd.Dir = moduleRoot
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("go build: %w", err)
	}
	return binPath, nil
}

func findModuleRoot() (string, error) {
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		return "", fmt.Errorf("go env GOMOD: %w", err)
	}
	goModPath := strings.TrimSpace(string(out))
	if goModPath == "" || goModPath == "/dev/null" {
		return "", fmt.Errorf("not in a go module")
	}
	return filepath.Dir(goModPath), nil
}

// pickFreeTCPPort returns a free localhost TCP port. Small race window
// between listener close and caller bind; safe for test scaffolding.
func pickFreeTCPPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port, nil
}

// waitForTCP polls addr until a TCP dial succeeds or the deadline
// expires. Used to confirm aetherlite is accepting connections.
func waitForTCP(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("never reached ready state within %s", timeout)
}
