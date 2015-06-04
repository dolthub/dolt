package types

// flatList is a quick 'n easy implementation of List.
// It should eventually be replaced by a chunking implementation.
type flatList struct {
	list []Value
}

func (l flatList) Len() uint64 {
	return uint64(len(l.list))
}

func (l flatList) Get(idx uint64) Value {
	return l.list[idx]
}

func (l flatList) Slice(start uint64, end uint64) List {
	return flatList{l.list[start:end]}
}

func (l flatList) Set(idx uint64, v Value) List {
	b := make([]Value, len(l.list))
	copy(b, l.list)
	b[idx] = v
	return flatList{b}
}

func (l flatList) Append(v ...Value) List {
	return flatList{append(l.list, v...)}
}

func (l flatList) Insert(idx uint64, v ...Value) List {
	b := make([]Value, len(l.list)+len(v))
	copy(b, l.list[:idx])
	copy(b[idx:], v)
	copy(b[idx+uint64(len(v)):], l.list[idx:])
	return flatList{b}
}

func (l flatList) Remove(start uint64, end uint64) List {
	b := make([]Value, uint64(len(l.list))-(end-start))
	copy(b, l.list[:start])
	copy(b[start:], l.list[end:])
	return flatList{b}
}

func (l flatList) RemoveAt(idx uint64) List {
	return l.Remove(idx, idx+1)
}

func (l flatList) Equals(other Value) bool {
	// TODO: Seems like this might be better to implement via content addressing.
	//
	// 1. Give List a Codec-like interface (not Codec itself because of circular deps)
	// 2. Give List (or maybe all values?) a Ref() method
	// 3. Ref works by using Codec to serialize the value, then caching the ref
	// 4. Equals works by comparing the value of Ref()
	//
	// The Codec-alike interface in (1) doesn't have to be a general purpose cache, I don't think. The client of this type probably knows whether he's about to send stuff someplace else (in which case he doesn't need a cache because he can just send the chunks immediately, or at worse put them in a temp dir), or whether he's not going to send them anywhere (in which case he doesn't need a cache because he doesn't need the data permanently).
	if other, ok := other.(List); ok {
		if l.Len() != other.Len() {
			return false
		}
		for i := uint64(0); i < l.Len(); i++ {
			if !l.Get(i).Equals(other.Get(i)) {
				return false
			}
		}
		return true
	}
	return false
}
