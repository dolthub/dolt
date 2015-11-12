package xlsx

type RefTable struct {
	indexedStrings []string
	knownStrings   map[string]int
	isWrite        bool
}

// NewSharedStringRefTable() creates a new, empty RefTable.
func NewSharedStringRefTable() *RefTable {
	rt := RefTable{}
	rt.knownStrings = make(map[string]int)
	return &rt
}

// MakeSharedStringRefTable() takes an xlsxSST struct and converts
// it's contents to an slice of strings used to refer to string values
// by numeric index - this is the model used within XLSX worksheet (a
// numeric reference is stored to a shared cell value).
func MakeSharedStringRefTable(source *xlsxSST) *RefTable {
	reftable := NewSharedStringRefTable()
	reftable.isWrite = false
	for _, si := range source.SI {
		if len(si.R) > 0 {
			newString := ""
			for j := 0; j < len(si.R); j++ {
				newString = newString + si.R[j].T
			}
			reftable.AddString(newString)
		} else {
			reftable.AddString(si.T)
		}
	}
	return reftable
}

// makeXlsxSST() takes a RefTable and returns and
// equivalent xlsxSST representation.
func (rt *RefTable) makeXLSXSST() xlsxSST {
	sst := xlsxSST{}
	sst.Count = len(rt.indexedStrings)
	sst.UniqueCount = sst.Count
	for _, ref := range rt.indexedStrings {
		si := xlsxSI{}
		si.T = ref
		sst.SI = append(sst.SI, si)
	}
	return sst
}

// Resolvesharedstring() looks up a string value by numeric index from
// a provided reference table (just a slice of strings in the correct
// order).  This function only exists to provide clarity or purpose
// via it's name.
func (rt *RefTable) ResolveSharedString(index int) string {
	return rt.indexedStrings[index]
}

// AddString adds a string to the reference table and return it's
// numeric index.  If the string already exists then it simply returns
// the existing index.
func (rt *RefTable) AddString(str string) int {
	if rt.isWrite {
		index, ok := rt.knownStrings[str]
		if ok {
			return index
		}
	}
	rt.indexedStrings = append(rt.indexedStrings, str)
	index := len(rt.indexedStrings) - 1
	rt.knownStrings[str] = index
	return index
}

func (rt *RefTable) Length() int {
	return len(rt.indexedStrings)
}
