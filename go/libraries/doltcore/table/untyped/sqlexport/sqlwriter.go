package sqlexport

import (
	"context"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"io"
	"path/filepath"
	"strings"
)

type SqlExportWriter struct {
	tableName       string
	sch             schema.Schema
	wr              io.WriteCloser
	writtenFirstRow bool
}

func OpenSQLExportWriter(path string, tableName string, fs filesys.WritableFS, sch schema.Schema) (*SqlExportWriter, error) {
	err := fs.MkDirs(filepath.Dir(path))
	if err != nil {
		return nil, err
	}

	wr, err := fs.OpenForWrite(path)
	if err != nil {
		return nil, err
	}

	return &SqlExportWriter{tableName: tableName, sch: sch, wr: wr}, nil
}

func (w *SqlExportWriter) GetSchema() schema.Schema {
	return w.sch
}

// WriteRow will write a row to a table
func (w *SqlExportWriter) WriteRow(ctx context.Context, r row.Row) error {
	if err := w.maybeWriteDropCreate(); err != nil {
			return err
	}

	return iohelp.WriteLine(w.wr, w.insertStatementForRow(r))
}

func (w *SqlExportWriter) maybeWriteDropCreate() error {
	if !w.writtenFirstRow {
		if err := iohelp.WriteLine(w.wr, w.dropCreateStatement()); err != nil {
			return err
		}
		w.writtenFirstRow = true
	}
	return nil
}

// Close should flush all writes, release resources being held
func (w *SqlExportWriter) Close(ctx context.Context) error {
	// exporting an empty table will not get any WriteRow calls, so write the drop / create here
	if err := w.maybeWriteDropCreate(); err != nil {
		return err
	}

	if w.wr != nil {
		return w.wr.Close()
	}
	return nil
}

func (w *SqlExportWriter) insertStatementForRow(r row.Row) string {
	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(w.tableName)
	b.WriteString(" ")

	b.WriteString("(")
	var seenOne bool
	w.sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		if seenOne {
			b.WriteRune(',')
		}
		b.WriteString(sql.QuoteIdentifier(col.Name))
		seenOne = true
		return false
	})
	b.WriteString(")")

	b.WriteString(" VALUES (")
	seenOne = false
	r.IterSchema(w.sch, func(tag uint64, val types.Value) (stop bool) {
		if seenOne {
			b.WriteRune(',')
		}
		b.WriteString(w.sqlString(val))
		seenOne = true
		return false
	})

	b.WriteString(");")

	return b.String()
}

func (w *SqlExportWriter) dropCreateStatement() string {
	var b strings.Builder
	b.WriteString("DROP TABLE IF EXISTS ")
	b.WriteString(sql.QuoteIdentifier(w.tableName))
	b.WriteString(";\n")
	b.WriteString(sql.SchemaAsCreateStmt(w.tableName, w.sch))

	return b.String()
}

func (w *SqlExportWriter) sqlString(value types.Value) string {
	if types.IsNull(value) {
		return "NULL"
	}

	switch value.Kind() {
	case types.BoolKind:
		if value.(types.Bool) {
			return "TRUE"
		} else {
			return "FALSE"
		}
	case types.UUIDKind:
		convFn := doltcore.GetConvFunc(value.Kind(), types.StringKind)
		str, _ := convFn(value)
		return "\"" + string(str.(types.String)) + "\""
	case types.StringKind:
		// TODO: escape quotes
		return "\"" + string(value.(types.String)) + "\""
	default:
		convFn := doltcore.GetConvFunc(value.Kind(), types.StringKind)
		str, _ := convFn(value)
		return string(str.(types.String))
	}
}