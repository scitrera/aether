package dbcompat

import (
	"context"
	"database/sql"
	"database/sql/driver"

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
	return c.conn.Prepare(rewriteQuery(query))
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
	if cp, ok := c.conn.(driver.ConnPrepareContext); ok {
		return cp.PrepareContext(ctx, rewritten)
	}
	return c.conn.Prepare(rewritten)
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
		return qc.QueryContext(ctx, rewritten, args)
	}
	// Fall back to prepare+query
	stmt, err := c.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	return stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
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
