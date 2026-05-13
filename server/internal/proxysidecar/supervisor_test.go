package proxysidecar

import (
	"context"
	"reflect"
	"runtime"
	"syscall"
	"testing"
	"time"
)

func TestSupervisor_NewSupervisor_RejectsEmptyArgv(t *testing.T) {
	if _, err := NewSupervisor(nil); err == nil {
		t.Fatalf("NewSupervisor(nil): expected error, got nil")
	}
	if _, err := NewSupervisor([]string{}); err == nil {
		t.Fatalf("NewSupervisor([]): expected error, got nil")
	}
}

func TestSupervisor_BadCommand_StartReturnsError(t *testing.T) {
	sup, err := NewSupervisor([]string{"/no/such/binary/anywhere"})
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	startErr := sup.Start(context.Background())
	if startErr == nil {
		t.Fatal("Start: expected error for missing binary, got nil")
	}
	// Done must close so callers waiting on it don't hang.
	select {
	case <-sup.Done():
	case <-time.After(time.Second):
		t.Fatal("Done: expected close after Start failure, timed out")
	}
}

func TestSupervisor_CleanExit_PropagatesZero(t *testing.T) {
	skipIfWindows(t)
	sup, err := NewSupervisor([]string{"sh", "-c", "exit 0"})
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitForDone(t, sup, 5*time.Second)
	if got := sup.ExitCode(); got != 0 {
		t.Fatalf("ExitCode: got %d, want 0", got)
	}
}

func TestSupervisor_NonZeroExit_PropagatesCode(t *testing.T) {
	skipIfWindows(t)
	sup, err := NewSupervisor([]string{"sh", "-c", "exit 42"})
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	waitForDone(t, sup, 5*time.Second)
	if got := sup.ExitCode(); got != 42 {
		t.Fatalf("ExitCode: got %d, want 42", got)
	}
}

func TestSupervisor_Signal_TerminatesChild(t *testing.T) {
	skipIfWindows(t)
	// `trap '' TERM` would mask the signal; we want the default action so the
	// child dies of SIGTERM and we can observe the 128+signum mapping.
	sup, err := NewSupervisor([]string{"sh", "-c", "sleep 30"})
	if err != nil {
		t.Fatalf("NewSupervisor: %v", err)
	}
	if err := sup.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := sup.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("Signal: %v", err)
	}
	waitForDone(t, sup, 5*time.Second)
	want := 128 + int(syscall.SIGTERM)
	if got := sup.ExitCode(); got != want {
		t.Fatalf("ExitCode: got %d, want %d (128+SIGTERM)", got, want)
	}
	// Signal after exit must be a no-op, not an error.
	if err := sup.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("Signal after exit: got %v, want nil", err)
	}
}

func TestSplitChildArgs(t *testing.T) {
	cases := []struct {
		name string
		in   []string
		head []string
		tail []string
	}{
		{
			name: "no separator",
			in:   []string{"prog", "-flag", "value"},
			head: []string{"prog", "-flag", "value"},
			tail: nil,
		},
		{
			name: "separator with command",
			in:   []string{"prog", "-flag", "--", "child", "-x"},
			head: []string{"prog", "-flag"},
			tail: []string{"child", "-x"},
		},
		{
			name: "trailing separator",
			in:   []string{"prog", "--"},
			head: []string{"prog"},
			tail: nil,
		},
		{
			name: "separator first arg after prog",
			in:   []string{"prog", "--", "child"},
			head: []string{"prog"},
			tail: []string{"child"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			head, tail := SplitChildArgs(tc.in)
			if !reflect.DeepEqual(head, tc.head) {
				t.Fatalf("head: got %#v, want %#v", head, tc.head)
			}
			if !reflect.DeepEqual(tail, tc.tail) {
				t.Fatalf("tail: got %#v, want %#v", tail, tc.tail)
			}
		})
	}
}

// waitForDone fails the test if the supervisor doesn't finish within d.
func waitForDone(t *testing.T, sup *Supervisor, d time.Duration) {
	t.Helper()
	select {
	case <-sup.Done():
	case <-time.After(d):
		t.Fatalf("supervisor did not finish within %v", d)
	}
}

func skipIfWindows(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("supervisor tests use POSIX sh and SIGTERM")
	}
}
