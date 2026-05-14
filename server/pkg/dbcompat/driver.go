package dbcompat

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"strings"
	"time"

	_ "modernc.org/sqlite" // register base "sqlite" driver
)

func init() {
	// Register a "sqlite_compat" driver that wraps the "sqlite" driver
	// with PostgreSQL→SQLite query rewriting.
	sql.Register("sqlite_compat", &compatDriver{})
}

// compatDriver wraps the modernc.org/sqlite driver with query rewriting.
type compatDriver struct{}

func (d *compatDriver) Open(dsn string) (driver.Conn, error) {
	// Get the underlying sqlite driver
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// We need to get a raw connection from the underlying driver.
	// Close the temporary *sql.DB wrapper — we'll use the driver directly.
	db.Close()

	// Use the registered sqlite driver directly
	sqliteDriver := findDriver("sqlite")
	if sqliteDriver == nil {
		return nil, &driverError{msg: "sqlite driver not found"}
	}
	conn, err := sqliteDriver.Open(dsn)
	if err != nil {
		return nil, err
	}
	return &compatConn{conn: conn}, nil
}

// compatConn wraps a driver.Conn to rewrite queries.
type compatConn struct {
	conn driver.Conn
}

func (c *compatConn) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.conn.Prepare(rewriteQuery(query))
	if err != nil {
		return nil, err
	}
	return &compatStmt{stmt: stmt}, nil
}

func (c *compatConn) Close() error {
	return c.conn.Close()
}

func (c *compatConn) Begin() (driver.Tx, error) {
	//nolint:staticcheck // Begin is required by driver.Conn interface
	return c.conn.Begin()
}

// Implement driver.ConnBeginTx for modern transaction support.
func (c *compatConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if cb, ok := c.conn.(driver.ConnBeginTx); ok {
		return cb.BeginTx(ctx, opts)
	}
	//nolint:staticcheck
	return c.conn.Begin()
}

// Implement driver.ConnPrepareContext for context-aware prepare.
func (c *compatConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	rewritten := rewriteQuery(query)
	var stmt driver.Stmt
	var err error
	if cp, ok := c.conn.(driver.ConnPrepareContext); ok {
		stmt, err = cp.PrepareContext(ctx, rewritten)
	} else {
		stmt, err = c.conn.Prepare(rewritten)
	}
	if err != nil {
		return nil, err
	}
	return &compatStmt{stmt: stmt}, nil
}

// Implement driver.ExecerContext for direct exec without prepare.
func (c *compatConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	rewritten := rewriteQuery(query)
	if ec, ok := c.conn.(driver.ExecerContext); ok {
		return ec.ExecContext(ctx, rewritten, args)
	}
	// Fall back to prepare+exec
	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.(driver.StmtExecContext).ExecContext(ctx, args)
}

// Implement driver.QueryerContext for direct query without prepare.
func (c *compatConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rewritten := rewriteQuery(query)
	if qc, ok := c.conn.(driver.QueryerContext); ok {
		rows, err := qc.QueryContext(ctx, rewritten, args)
		if err != nil {
			return nil, err
		}
		return wrapRows(rows), nil
	}
	// Fall back to prepare+query
	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
	if err != nil {
		return nil, err
	}
	return wrapRows(rows), nil
}

// Implement driver.Pinger
func (c *compatConn) Ping(ctx context.Context) error {
	if p, ok := c.conn.(driver.Pinger); ok {
		return p.Ping(ctx)
	}
	return nil
}

// Implement driver.SessionResetter
func (c *compatConn) ResetSession(ctx context.Context) error {
	if sr, ok := c.conn.(driver.SessionResetter); ok {
		return sr.ResetSession(ctx)
	}
	return nil
}

// Implement driver.Validator
func (c *compatConn) IsValid() bool {
	if v, ok := c.conn.(driver.Validator); ok {
		return v.IsValid()
	}
	return true
}

type driverError struct {
	msg string
}

func (e *driverError) Error() string {
	return e.msg
}

// findDriver returns the registered sql driver by name.
func findDriver(name string) driver.Driver {
	for _, d := range sql.Drivers() {
		if d == name {
			// We need to open a dummy db to get the driver
			db, err := sql.Open(name, "")
			if err != nil {
				return nil
			}
			drv := db.Driver()
			db.Close()
			return drv
		}
	}
	return nil
}

// =============================================================================
// Statement wrapper — produces compatRows from Query{,Context}.
// =============================================================================

// compatStmt wraps a driver.Stmt so that any rows it produces get the
// timestamp-coercion wrapper installed. Exec paths pass through untouched.
type compatStmt struct {
	stmt driver.Stmt
}

func (s *compatStmt) Close() error                                    { return s.stmt.Close() }
func (s *compatStmt) NumInput() int                                   { return s.stmt.NumInput() }
func (s *compatStmt) Exec(args []driver.Value) (driver.Result, error) { return s.stmt.Exec(args) } //nolint:staticcheck

func (s *compatStmt) Query(args []driver.Value) (driver.Rows, error) {
	rows, err := s.stmt.Query(args) //nolint:staticcheck // satisfy driver.Stmt
	if err != nil {
		return nil, err
	}
	return wrapRows(rows), nil
}

func (s *compatStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if ec, ok := s.stmt.(driver.StmtExecContext); ok {
		return ec.ExecContext(ctx, args)
	}
	// Fall back to the deprecated value path.
	values := make([]driver.Value, len(args))
	for i, a := range args {
		values[i] = a.Value
	}
	return s.stmt.Exec(values) //nolint:staticcheck
}

func (s *compatStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	var rows driver.Rows
	var err error
	if qc, ok := s.stmt.(driver.StmtQueryContext); ok {
		rows, err = qc.QueryContext(ctx, args)
	} else {
		values := make([]driver.Value, len(args))
		for i, a := range args {
			values[i] = a.Value
		}
		rows, err = s.stmt.Query(values) //nolint:staticcheck
	}
	if err != nil {
		return nil, err
	}
	return wrapRows(rows), nil
}

// =============================================================================
// Rows wrapper — coerces TEXT-encoded timestamps to time.Time at Scan time.
//
// SQLite stores all timestamps as TEXT (see migrations/sqlite/001_full_schema.sql:
// `created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP` etc.). modernc.org/sqlite
// hands those values back to database/sql as `string`, but the aether codebase
// scans them into *time.Time, which database/sql refuses ("unsupported Scan,
// storing driver.Value type string into type *time.Time").
//
// We detect timestamp columns by NAME (sqlite returns "TEXT" for everything
// via ColumnTypeDatabaseTypeName, so type-based detection is useless) and
// parse them into time.Time before handing the row back to the caller. The
// caller's `rows.Scan(&t)` then succeeds because the driver.Value is already
// a time.Time.
//
// False positives are benign: a non-timestamp string column whose name
// happens to match the heuristic will be tried against the well-known
// sqlite timestamp formats, all parses will fail, and the original string
// is passed through unchanged.
// =============================================================================

// timestampFormats lists the on-disk encodings sqlite uses for CURRENT_TIMESTAMP
// and that the aether codebase writes via parameter binding. Tried in order;
// first parse wins.
//
// The "go-stringified" formats catch values stored by modernc.org/sqlite
// when a Go *time.Time is bound as a parameter without explicit
// formatting: modernc falls back to time.Time.String(), which yields
// "2006-01-02 15:04:05.999999999 -0700 MST". A monotonic-clock suffix
// (" m=+x.y") is stripped before parsing (see parseTimestamp).
var timestampFormats = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999 -0700 MST",
	"2006-01-02 15:04:05 -0700 MST",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05.999999",
	"2006-01-02 15:04:05",
}

// timestampSuffixes / timestampPrefixes drive the column-name heuristic.
// Suffix "_at" catches the canonical {created,updated,granted,...}_at family.
// Prefixes catch the handful of timestamp columns that don't follow the
// _at suffix convention (last_heartbeat, connected_at via suffix, etc.).
var (
	timestampSuffixes = []string{"_at"}
	timestampPrefixes = []string{
		"connected_",
		"last_",
		"updated_",
		"created_",
		"granted_",
		"completed_",
		"failed_",
		"scheduled_",
		"revoked_",
		"refreshed_",
		"renewed_",
		"expires_",
		"renewable_",
		"started_",
		"applied_",
		"assigned_",
		"disconnected_",
		"reprocessed_",
		"claimed_",
		"enqueued_",
		"fires_",
		"fired_",
		"timestamp",
	}
)

// isTimestampColumn returns true when the column name matches any of the
// suffix or prefix heuristics defined above.
func isTimestampColumn(name string) bool {
	n := strings.ToLower(name)
	for _, suf := range timestampSuffixes {
		if strings.HasSuffix(n, suf) {
			return true
		}
	}
	for _, pre := range timestampPrefixes {
		if strings.HasPrefix(n, pre) {
			return true
		}
	}
	return false
}

// compatRows wraps a driver.Rows so Next() coerces string-encoded timestamps
// in timestamp columns into time.Time before they reach database/sql.
type compatRows struct {
	rows     driver.Rows
	cols     []string
	tsColumn []bool // parallel to cols; true => candidate for timestamp coercion
}

// wrapRows builds a compatRows around any driver.Rows. Returns the input
// unchanged when it's already a *compatRows (defensive — shouldn't happen).
func wrapRows(rows driver.Rows) driver.Rows {
	if rows == nil {
		return nil
	}
	if _, ok := rows.(*compatRows); ok {
		return rows
	}
	cols := rows.Columns()
	tsColumn := make([]bool, len(cols))
	for i, c := range cols {
		tsColumn[i] = isTimestampColumn(c)
	}
	return &compatRows{rows: rows, cols: cols, tsColumn: tsColumn}
}

func (r *compatRows) Columns() []string { return r.cols }
func (r *compatRows) Close() error      { return r.rows.Close() }

func (r *compatRows) Next(dest []driver.Value) error {
	if err := r.rows.Next(dest); err != nil {
		return err
	}
	for i, isTS := range r.tsColumn {
		if !isTS {
			continue
		}
		if i >= len(dest) {
			break
		}
		s, ok := dest[i].(string)
		if !ok {
			// NULL (nil) or already a time.Time / []byte — leave it.
			continue
		}
		if t, parsed := parseTimestamp(s); parsed {
			dest[i] = t
		}
		// Parse failed: pass the original string through so the standard
		// library surfaces the real type-mismatch error rather than silently
		// substituting a zero time.
	}
	return nil
}

// parseTimestamp tries each known sqlite timestamp encoding in order.
// Returns (zero, false) when none match so the caller can leave the
// original value untouched.
//
// Strips Go's monotonic-clock suffix (" m=+...") before parsing so values
// stored via time.Time.String() (modernc.org/sqlite's default binding
// for time.Time parameters) round-trip cleanly.
func parseTimestamp(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	// Drop monotonic-clock tail: "...UTC m=+1.234567"
	if i := strings.Index(s, " m=+"); i >= 0 {
		s = s[:i]
	} else if i := strings.Index(s, " m=-"); i >= 0 {
		s = s[:i]
	}
	for _, layout := range timestampFormats {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// _ = io.EOF documents that callers should see io.EOF propagate cleanly
// from the underlying rows.Next implementation; we don't shadow it.
var _ = io.EOF
