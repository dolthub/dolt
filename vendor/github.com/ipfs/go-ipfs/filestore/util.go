package filestore

import (
	"fmt"
	"sort"

	"github.com/ipfs/go-ipfs/blocks/blockstore"
	pb "github.com/ipfs/go-ipfs/filestore/pb"
	dshelp "github.com/ipfs/go-ipfs/thirdparty/ds-help"

	cid "gx/ipfs/QmNp85zy9RLrQ5oQD4hPyS39ezrrXpcaa7R4Y9kxdWQLLQ/go-cid"
	ds "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore"
	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"
)

// Status is used to identify the state of the block data referenced
// by a FilestoreNode. Among other places, it is used by CorruptReferenceError.
type Status int32

// These are the supported Status codes.
const (
	StatusOk           Status = 0
	StatusFileError    Status = 10 // Backing File Error
	StatusFileNotFound Status = 11 // Backing File Not Found
	StatusFileChanged  Status = 12 // Contents of the file changed
	StatusOtherError   Status = 20 // Internal Error, likely corrupt entry
	StatusKeyNotFound  Status = 30
)

// String provides a human-readable representation for Status codes.
func (s Status) String() string {
	switch s {
	case StatusOk:
		return "ok"
	case StatusFileError:
		return "error"
	case StatusFileNotFound:
		return "no-file"
	case StatusFileChanged:
		return "changed"
	case StatusOtherError:
		return "ERROR"
	case StatusKeyNotFound:
		return "missing"
	default:
		return "???"
	}
}

// Format returns the status formatted as a string
// with leading 0s.
func (s Status) Format() string {
	return fmt.Sprintf("%-7s", s.String())
}

// ListRes wraps the response of the List*() functions, which
// allows to obtain and verify blocks stored by the FileManager
// of a Filestore. It includes information about the referenced
// block.
type ListRes struct {
	Status   Status
	ErrorMsg string
	Key      *cid.Cid
	FilePath string
	Offset   uint64
	Size     uint64
}

// FormatLong returns a human readable string for a ListRes object.
func (r *ListRes) FormatLong() string {
	switch {
	case r.Key == nil:
		return "<corrupt key>"
	case r.FilePath == "":
		return r.Key.String()
	default:
		return fmt.Sprintf("%-50s %6d %s %d", r.Key, r.Size, r.FilePath, r.Offset)
	}
}

// List fetches the block with the given key from the Filemanager
// of the given Filestore and returns a ListRes object with the information.
// List does not verify that the reference is valid or whether the
// raw data is accesible. See Verify().
func List(fs *Filestore, key *cid.Cid) *ListRes {
	return list(fs, false, key)
}

// ListAll returns a function as an iterator which, once invoked, returns
// one by one each block in the Filestore's FileManager.
// ListAll does not verify that the references are valid or whether
// the raw data is accessible. See VerifyAll().
func ListAll(fs *Filestore, fileOrder bool) (func() *ListRes, error) {
	if fileOrder {
		return listAllFileOrder(fs, false)
	}
	return listAll(fs, false)
}

// Verify fetches the block with the given key from the Filemanager
// of the given Filestore and returns a ListRes object with the information.
// Verify makes sure that the reference is valid and the block data can be
// read.
func Verify(fs *Filestore, key *cid.Cid) *ListRes {
	return list(fs, true, key)
}

// VerifyAll returns a function as an iterator which, once invoked,
// returns one by one each block in the Filestore's FileManager.
// VerifyAll checks that the reference is valid and that the block data
// can be read.
func VerifyAll(fs *Filestore, fileOrder bool) (func() *ListRes, error) {
	if fileOrder {
		return listAllFileOrder(fs, true)
	}
	return listAll(fs, true)
}

func list(fs *Filestore, verify bool, key *cid.Cid) *ListRes {
	dobj, err := fs.fm.getDataObj(key)
	if err != nil {
		return mkListRes(key, nil, err)
	}
	if verify {
		_, err = fs.fm.readDataObj(key, dobj)
	}
	return mkListRes(key, dobj, err)
}

func listAll(fs *Filestore, verify bool) (func() *ListRes, error) {
	q := dsq.Query{}
	qr, err := fs.fm.ds.Query(q)
	if err != nil {
		return nil, err
	}

	return func() *ListRes {
		cid, dobj, err := next(qr)
		if dobj == nil && err == nil {
			return nil
		} else if err == nil && verify {
			_, err = fs.fm.readDataObj(cid, dobj)
		}
		return mkListRes(cid, dobj, err)
	}, nil
}

func next(qr dsq.Results) (*cid.Cid, *pb.DataObj, error) {
	v, ok := qr.NextSync()
	if !ok {
		return nil, nil, nil
	}

	k := ds.RawKey(v.Key)
	c, err := dshelp.DsKeyToCid(k)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding cid from filestore: %s", err)
	}

	dobj, err := unmarshalDataObj(v.Value)
	if err != nil {
		return c, nil, err
	}

	return c, dobj, nil
}

func listAllFileOrder(fs *Filestore, verify bool) (func() *ListRes, error) {
	q := dsq.Query{}
	qr, err := fs.fm.ds.Query(q)
	if err != nil {
		return nil, err
	}

	var entries listEntries

	for {
		v, ok := qr.NextSync()
		if !ok {
			break
		}
		dobj, err := unmarshalDataObj(v.Value)
		if err != nil {
			entries = append(entries, &listEntry{
				dsKey: v.Key,
				err:   err,
			})
		} else {
			entries = append(entries, &listEntry{
				dsKey:    v.Key,
				filePath: dobj.GetFilePath(),
				offset:   dobj.GetOffset(),
				size:     dobj.GetSize_(),
			})
		}
	}
	sort.Sort(entries)

	i := 0
	return func() *ListRes {
		if i >= len(entries) {
			return nil
		}
		v := entries[i]
		i++
		// attempt to convert the datastore key to a CID,
		// store the error but don't use it yet
		cid, keyErr := dshelp.DsKeyToCid(ds.RawKey(v.dsKey))
		// first if they listRes already had an error return that error
		if v.err != nil {
			return mkListRes(cid, nil, v.err)
		}
		// now reconstruct the DataObj
		dobj := pb.DataObj{
			FilePath: &v.filePath,
			Offset:   &v.offset,
			Size_:    &v.size,
		}
		// now if we could not convert the datastore key return that
		// error
		if keyErr != nil {
			return mkListRes(cid, &dobj, keyErr)
		}
		// finally verify the dataobj if requested
		var err error
		if verify {
			_, err = fs.fm.readDataObj(cid, &dobj)
		}
		return mkListRes(cid, &dobj, err)
	}, nil
}

type listEntry struct {
	filePath string
	offset   uint64
	dsKey    string
	size     uint64
	err      error
}

type listEntries []*listEntry

func (l listEntries) Len() int      { return len(l) }
func (l listEntries) Swap(i, j int) { l[i], l[j] = l[j], l[i] }
func (l listEntries) Less(i, j int) bool {
	if l[i].filePath == l[j].filePath {
		if l[i].offset == l[j].offset {
			return l[i].dsKey < l[j].dsKey
		}
		return l[i].offset < l[j].offset
	}
	return l[i].filePath < l[j].filePath
}

func mkListRes(c *cid.Cid, d *pb.DataObj, err error) *ListRes {
	status := StatusOk
	errorMsg := ""
	if err != nil {
		if err == ds.ErrNotFound || err == blockstore.ErrNotFound {
			status = StatusKeyNotFound
		} else if err, ok := err.(*CorruptReferenceError); ok {
			status = err.Code
		} else {
			status = StatusOtherError
		}
		errorMsg = err.Error()
	}
	if d == nil {
		return &ListRes{
			Status:   status,
			ErrorMsg: errorMsg,
			Key:      c,
		}
	}

	return &ListRes{
		Status:   status,
		ErrorMsg: errorMsg,
		Key:      c,
		FilePath: *d.FilePath,
		Size:     *d.Size_,
		Offset:   *d.Offset,
	}
}
