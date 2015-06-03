package types

// Value is implemented by every noms value
type Value interface {
	Equals(other Value) bool
}
