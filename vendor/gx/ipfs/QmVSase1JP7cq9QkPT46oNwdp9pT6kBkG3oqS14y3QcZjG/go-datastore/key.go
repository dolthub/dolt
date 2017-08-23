package datastore

import (
	"path"
	"strings"

	dsq "gx/ipfs/QmVSase1JP7cq9QkPT46oNwdp9pT6kBkG3oqS14y3QcZjG/go-datastore/query"

	"gx/ipfs/QmcyaFHbyiZfoX5GTpcqqCPYmbjYNAhRDekXSJPFHdYNSV/go.uuid"
)

/*
A Key represents the unique identifier of an object.
Our Key scheme is inspired by file systems and Google App Engine key model.

Keys are meant to be unique across a system. Keys are hierarchical,
incorporating more and more specific namespaces. Thus keys can be deemed
'children' or 'ancestors' of other keys::

    Key("/Comedy")
    Key("/Comedy/MontyPython")

Also, every namespace can be parametrized to embed relevant object
information. For example, the Key `name` (most specific namespace) could
include the object type::

    Key("/Comedy/MontyPython/Actor:JohnCleese")
    Key("/Comedy/MontyPython/Sketch:CheeseShop")
    Key("/Comedy/MontyPython/Sketch:CheeseShop/Character:Mousebender")

*/
type Key struct {
	string
}

// NewKey constructs a key from string. it will clean the value.
func NewKey(s string) Key {
	k := Key{s}
	k.Clean()
	return k
}

// RawKey creates a new Key without safety checking the input. Use with care.
func RawKey(s string) Key {
	// accept an empty string and fix it to avoid special cases
	// elsewhere
	if len(s) == 0 {
		return Key{"/"}
	}

	// perform a quick sanity check that the key is in the correct
	// format, if it is not then it is a programmer error and it is
	// okay to panic
	if len(s) == 0 || s[0] != '/' || (len(s) > 1 && s[len(s)-1] == '/') {
		panic("invalid datastore key: " + s)
	}

	return Key{s}
}

// KeyWithNamespaces constructs a key out of a namespace slice.
func KeyWithNamespaces(ns []string) Key {
	return NewKey(strings.Join(ns, "/"))
}

// Clean up a Key, using path.Clean.
func (k *Key) Clean() {
	switch {
	case len(k.string) == 0:
		k.string = "/"
	case k.string[0] == '/':
		k.string = path.Clean(k.string)
	default:
		k.string = path.Clean("/" + k.string)
	}
}

// Strings is the string value of Key
func (k Key) String() string {
	return k.string
}

// Bytes returns the string value of Key as a []byte
func (k Key) Bytes() []byte {
	return []byte(k.string)
}

// Equal checks equality of two keys
func (k Key) Equal(k2 Key) bool {
	return k.string == k2.string
}

// Less checks whether this key is sorted lower than another.
func (k Key) Less(k2 Key) bool {
	list1 := k.List()
	list2 := k2.List()
	for i, c1 := range list1 {
		if len(list2) < (i + 1) {
			return false
		}

		c2 := list2[i]
		if c1 < c2 {
			return true
		} else if c1 > c2 {
			return false
		}
		// c1 == c2, continue
	}

	// list1 is shorter or exactly the same.
	return len(list1) < len(list2)
}

// List returns the `list` representation of this Key.
//   NewKey("/Comedy/MontyPython/Actor:JohnCleese").List()
//   ["Comedy", "MontyPythong", "Actor:JohnCleese"]
func (k Key) List() []string {
	return strings.Split(k.string, "/")[1:]
}

// Reverse returns the reverse of this Key.
//   NewKey("/Comedy/MontyPython/Actor:JohnCleese").Reverse()
//   NewKey("/Actor:JohnCleese/MontyPython/Comedy")
func (k Key) Reverse() Key {
	l := k.List()
	r := make([]string, len(l), len(l))
	for i, e := range l {
		r[len(l)-i-1] = e
	}
	return KeyWithNamespaces(r)
}

// Namespaces returns the `namespaces` making up this Key.
//   NewKey("/Comedy/MontyPython/Actor:JohnCleese").Namespaces()
//   ["Comedy", "MontyPython", "Actor:JohnCleese"]
func (k Key) Namespaces() []string {
	return k.List()
}

// BaseNamespace returns the "base" namespace of this key (path.Base(filename))
//   NewKey("/Comedy/MontyPython/Actor:JohnCleese").BaseNamespace()
//   "Actor:JohnCleese"
func (k Key) BaseNamespace() string {
	n := k.Namespaces()
	return n[len(n)-1]
}

// Type returns the "type" of this key (value of last namespace).
//   NewKey("/Comedy/MontyPython/Actor:JohnCleese").Type()
//   "Actor"
func (k Key) Type() string {
	return NamespaceType(k.BaseNamespace())
}

// Name returns the "name" of this key (field of last namespace).
//   NewKey("/Comedy/MontyPython/Actor:JohnCleese").Name()
//   "JohnCleese"
func (k Key) Name() string {
	return NamespaceValue(k.BaseNamespace())
}

// Instance returns an "instance" of this type key (appends value to namespace).
//   NewKey("/Comedy/MontyPython/Actor").Instance("JohnClesse")
//   NewKey("/Comedy/MontyPython/Actor:JohnCleese")
func (k Key) Instance(s string) Key {
	return NewKey(k.string + ":" + s)
}

// Path returns the "path" of this key (parent + type).
//   NewKey("/Comedy/MontyPython/Actor:JohnCleese").Path()
//   NewKey("/Comedy/MontyPython/Actor")
func (k Key) Path() Key {
	s := k.Parent().string + "/" + NamespaceType(k.BaseNamespace())
	return NewKey(s)
}

// Parent returns the `parent` Key of this Key.
//   NewKey("/Comedy/MontyPython/Actor:JohnCleese").Parent()
//   NewKey("/Comedy/MontyPython")
func (k Key) Parent() Key {
	n := k.List()
	if len(n) == 1 {
		return RawKey("/")
	}
	return NewKey(strings.Join(n[:len(n)-1], "/"))
}

// Child returns the `child` Key of this Key.
//   NewKey("/Comedy/MontyPython").Child(NewKey("Actor:JohnCleese"))
//   NewKey("/Comedy/MontyPython/Actor:JohnCleese")
func (k Key) Child(k2 Key) Key {
	switch {
	case k.string == "/":
		return k2
	case k2.string == "/":
		return k
	default:
		return RawKey(k.string + k2.string)
	}
}

// ChildString returns the `child` Key of this Key -- string helper.
//   NewKey("/Comedy/MontyPython").ChildString("Actor:JohnCleese")
//   NewKey("/Comedy/MontyPython/Actor:JohnCleese")
func (k Key) ChildString(s string) Key {
	return NewKey(k.string + "/" + s)
}

// IsAncestorOf returns whether this key is a prefix of `other`
//   NewKey("/Comedy").IsAncestorOf("/Comedy/MontyPython")
//   true
func (k Key) IsAncestorOf(other Key) bool {
	if other.string == k.string {
		return false
	}
	return strings.HasPrefix(other.string, k.string)
}

// IsDescendantOf returns whether this key contains another as a prefix.
//   NewKey("/Comedy/MontyPython").IsDescendantOf("/Comedy")
//   true
func (k Key) IsDescendantOf(other Key) bool {
	if other.string == k.string {
		return false
	}
	return strings.HasPrefix(k.string, other.string)
}

// IsTopLevel returns whether this key has only one namespace.
func (k Key) IsTopLevel() bool {
	return len(k.List()) == 1
}

// RandomKey returns a randomly (uuid) generated key.
//   RandomKey()
//   NewKey("/f98719ea086343f7b71f32ea9d9d521d")
func RandomKey() Key {
	return NewKey(strings.Replace(uuid.NewV4().String(), "-", "", -1))
}

/*
A Key Namespace is like a path element.
A namespace can optionally include a type (delimited by ':')

    > NamespaceValue("Song:PhilosopherSong")
    PhilosopherSong
    > NamespaceType("Song:PhilosopherSong")
    Song
    > NamespaceType("Music:Song:PhilosopherSong")
    Music:Song
*/

// NamespaceType is the first component of a namespace. `foo` in `foo:bar`
func NamespaceType(namespace string) string {
	parts := strings.Split(namespace, ":")
	if len(parts) < 2 {
		return ""
	}
	return strings.Join(parts[0:len(parts)-1], ":")
}

// NamespaceValue returns the last component of a namespace. `baz` in `f:b:baz`
func NamespaceValue(namespace string) string {
	parts := strings.Split(namespace, ":")
	return parts[len(parts)-1]
}

// KeySlice attaches the methods of sort.Interface to []Key,
// sorting in increasing order.
type KeySlice []Key

func (p KeySlice) Len() int           { return len(p) }
func (p KeySlice) Less(i, j int) bool { return p[i].Less(p[j]) }
func (p KeySlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// EntryKeys
func EntryKeys(e []dsq.Entry) []Key {
	ks := make([]Key, len(e))
	for i, e := range e {
		ks[i] = NewKey(e.Key)
	}
	return ks
}
