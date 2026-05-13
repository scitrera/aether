// Supervisor wraps a child process underneath the sidecar. When the
// proxy-sidecar binary is invoked as `proxy-sidecar [opts] -- <cmd> [args...]`
// the trailing argv is handed to a Supervisor that owns the child's
// lifecycle: it is started after the runner's surfaces are up, and its exit
// is the trigger for the whole process to drain and exit with the child's
// status. The supervisor is intentionally independent of Runner and the
// gateway connection — it is a process-level concern, not a fourth surface.
package proxysidecar

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
)

// Supervisor manages the optional wrapped child process.
type Supervisor struct {
	argv []string

	startOnce sync.Once
	startErr  error

	cmd      *exec.Cmd
	doneCh   chan struct{}
	exitCode int
}

// NewSupervisor returns a Supervisor that will exec argv when Start is
// called. argv must be non-empty; argv[0] is the executable.
func NewSupervisor(argv []string) (*Supervisor, error) {
	if len(argv) == 0 {
		return nil, errors.New("supervisor: argv is empty")
	}
	return &Supervisor{
		argv:   argv,
		doneCh: make(chan struct{}),
	}, nil
}

// Start spawns the child. The child inherits the parent's stdio and
// environment and is placed in its own process group so signals delivered
// to the controlling terminal hit the parent only — the parent is
// responsible for forwarding them via Signal. Start is safe to call once;
// subsequent calls are no-ops that return the original error.
//
// The supplied context is intentionally unused for cancellation: the parent
// goroutine's signal handler decides when (and how) to terminate the
// child. Plumbing ctx into exec.CommandContext would otherwise race the
// signal-forwarding path and replace the child's reported status with a
// terse "signal: killed".
func (s *Supervisor) Start(_ context.Context) error {
	s.startOnce.Do(func() {
		cmd := exec.Command(s.argv[0], s.argv[1:]...)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Env = os.Environ()
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

		if err := cmd.Start(); err != nil {
			s.startErr = fmt.Errorf("supervisor: start %q: %w", s.argv[0], err)
			s.exitCode = 1
			close(s.doneCh)
			return
		}

		s.cmd = cmd
		go s.wait()
	})
	return s.startErr
}

// wait blocks on cmd.Wait, decodes the exit status, and signals doneCh.
func (s *Supervisor) wait() {
	err := s.cmd.Wait()
	s.exitCode = decodeExitStatus(s.cmd.ProcessState, err)
	close(s.doneCh)
}

// Done is closed when the child exits (or when Start failed). Safe to
// receive on before Start; in that case it remains open until Start is
// invoked.
func (s *Supervisor) Done() <-chan struct{} { return s.doneCh }

// ExitCode returns the child's exit status. Only meaningful after Done is
// closed. Returns 1 when Start itself failed.
func (s *Supervisor) ExitCode() int { return s.exitCode }

// Signal forwards sig to the child process. No-op (returns nil) when the
// child has already exited or was never started.
func (s *Supervisor) Signal(sig os.Signal) error {
	select {
	case <-s.doneCh:
		return nil
	default:
	}
	if s.cmd == nil || s.cmd.Process == nil {
		return nil
	}
	if err := s.cmd.Process.Signal(sig); err != nil {
		// os: process already finished is not actionable for the caller.
		if errors.Is(err, os.ErrProcessDone) {
			return nil
		}
		return err
	}
	return nil
}

// decodeExitStatus collapses (ProcessState, Wait error) into a single int
// using POSIX conventions. Pure exits return their code; signal-killed
// children return 128+signum; anything we can't classify returns 1.
func decodeExitStatus(state *os.ProcessState, waitErr error) int {
	if state != nil {
		if ws, ok := state.Sys().(syscall.WaitStatus); ok {
			if ws.Signaled() {
				return 128 + int(ws.Signal())
			}
			if ws.Exited() {
				return ws.ExitStatus()
			}
		}
		if code := state.ExitCode(); code >= 0 {
			return code
		}
	}
	if waitErr == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		if code := exitErr.ExitCode(); code >= 0 {
			return code
		}
	}
	return 1
}

// SplitChildArgs scans args for the first "--" sentinel and returns the
// head (everything before it, including args[0]) and the tail (everything
// after it). When "--" is absent the head is args unchanged and the tail
// is nil. A trailing "--" with no following argv is treated as "no child"
// — we return nil for the tail rather than an empty slice so callers can
// distinguish "supervisor mode without command" (an error case) from
// "supervisor mode not requested".
func SplitChildArgs(args []string) (head, tail []string) {
	for i, a := range args {
		if a == "--" {
			head = append([]string{}, args[:i]...)
			if i+1 < len(args) {
				tail = append([]string{}, args[i+1:]...)
			}
			return head, tail
		}
	}
	return args, nil
}
