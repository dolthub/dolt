package ref

// MarshalableRef is a wrapper that provides the marshaling and unmarshaling of DoltRefs as strings within json.
type MarshalableRef struct {
	Ref DoltRef
}

// MarshalJSON marshal the ref as a string
func (mr MarshalableRef) MarshalJSON() ([]byte, error) {
	if mr.Ref == nil {
		return []byte{}, nil
	}

	return MarshalJSON(mr.Ref)
}

// UnmarshalJSON unmarshals the ref from a string
func (mr *MarshalableRef) UnmarshalJSON(data []byte) error {
	if len(data) == 0 {
		return nil
	}

	dref, err := Parse(string(data[1 : len(data)-1]))

	if err != nil {
		return err
	}

	mr.Ref = dref

	return nil
}
