package ldio

import "os"

type OpenType int

const (
	Read OpenType = iota
	WriteNew
)

type OpenFileOp struct {
	FilePath string
	OpType   OpenType
}

func NewOpenReadOp(filePath string) *OpenFileOp {
	return &OpenFileOp{filePath, Read}
}

func NewCreateForWriteOp(filePath string) *OpenFileOp {
	return &OpenFileOp{filePath, WriteNew}
}

func closeAll(files []*os.File) {
	for _, f := range files {
		f.Close()
	}
}

func OpenAll(items []*OpenFileOp) ([]*os.File, error) {
	openedFiles := make([]*os.File, len(items))
	for i, item := range items {
		var f *os.File
		var err error
		switch item.OpType {
		case Read:
			f, err = os.Open(item.FilePath)
		case WriteNew:
			f, err = os.OpenFile(item.FilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
		}

		if err != nil {
			closeAll(openedFiles)
			return nil, err
		}

		openedFiles[i] = f
	}

	return openedFiles, nil
}

func OpenReaderCreateWriter(in, out string) (*os.File, *os.File, error) {
	files, err := OpenAll([]*OpenFileOp{
		NewOpenReadOp(in),
		NewCreateForWriteOp(out),
	})

	if err != nil {
		return nil, nil, err
	}

	return files[0], files[1], err
}
