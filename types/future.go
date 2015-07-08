package types

type Future interface {
	Value
	// Returned value guaranteed to not be a Future.
	Deref() (Value, error)
}
