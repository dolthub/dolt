package ref

type MarshalableRef struct {
	Ref DoltRef
}

func (mr MarshalableRef) MarshalJSON() ([]byte, error) {
	if mr.Ref == nil {
		return []byte{}, nil
	}

	return MarshalJSON(mr.Ref)
}

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
