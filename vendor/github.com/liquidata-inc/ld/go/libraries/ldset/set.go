package ldset

import "strings"

var emptyInstance = struct{}{}

type StrSet struct {
	items map[string]interface{}
}

func NewSet(items []string) *StrSet {
	s := &StrSet{make(map[string]interface{}, len(items))}

	if items != nil {
		for _, item := range items {
			s.items[item] = emptyInstance
		}
	}

	return s
}

func (s *StrSet) Add(item string) {
	s.items[item] = emptyInstance
}

func (s *StrSet) Contains(item string) bool {
	_, present := s.items[item]
	return present
}

func (s *StrSet) Size() int {
	return len(s.items)
}

func (s *StrSet) AsSlice() []string {
	size := len(s.items)
	sl := make([]string, size)

	i := 0
	for k := range s.items {
		sl[i] = k
		i++
	}

	return sl
}

func (s *StrSet) Iterate(callBack func(string) bool) {
	for k := range s.items {
		if !callBack(k) {
			break
		}
	}
}

func (s *StrSet) JoinStrings(sep string) string {
	return strings.Join(s.AsSlice(), sep)
}

func Unique(strs []string) []string {
	return NewSet(strs).AsSlice()
}
