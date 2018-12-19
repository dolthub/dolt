package blobstore

// NotFound is an error type used only when a key is not found in a Blobstore.
type NotFound struct {
	Key string
}

// Error returns the key which was not found
func (nf NotFound) Error() string {
	return nf.Key
}

// IsNotFoundError is a helper method used to determine if returned errors resulted
// because the key didn't exist as opposed to something going wrong.
func IsNotFoundError(err error) bool {
	_, ok := err.(NotFound)

	return ok
}

// CheckAndPutError is an error type used when CheckAndPut fails because of a version
// mismatch.
type CheckAndPutError struct {
	Key             string
	ExpectedVersion string
	ActualVersion   string
}

// Error (Required method of error) returns an error message for debugging
func (err CheckAndPutError) Error() string {
	return "Blob: \"" + err.Key + "\" expected: \"" + err.ExpectedVersion + "\" actual: \"" + err.ActualVersion + "\""
}

// IsCheckAndPutError is a helper method used to determine if CheckAndPut errors
// resulted because of version mismatches (Which happens when you have multiple)
// writers of a blob with a given key.
func IsCheckAndPutError(err error) bool {
	_, ok := err.(CheckAndPutError)

	return ok
}
