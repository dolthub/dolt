package keystore

import ci "gx/ipfs/QmaPbCnUMBohSGo3KnxEa2bHqyJVVeEEcwtqJAYxerieBo/go-libp2p-crypto"

type MemKeystore struct {
	keys map[string]ci.PrivKey
}

func NewMemKeystore() *MemKeystore {
	return &MemKeystore{make(map[string]ci.PrivKey)}
}

// Has return whether or not a key exist in the Keystore
func (mk *MemKeystore) Has(name string) (bool, error) {
	_, ok := mk.keys[name]
	return ok, nil
}

// Put store a key in the Keystore
func (mk *MemKeystore) Put(name string, k ci.PrivKey) error {
	if err := validateName(name); err != nil {
		return err
	}

	_, ok := mk.keys[name]
	if ok {
		return ErrKeyExists
	}

	mk.keys[name] = k
	return nil
}

// Get retrieve a key from the Keystore
func (mk *MemKeystore) Get(name string) (ci.PrivKey, error) {
	if err := validateName(name); err != nil {
		return nil, err
	}

	k, ok := mk.keys[name]
	if !ok {
		return nil, ErrNoSuchKey
	}

	return k, nil
}

// Delete remove a key from the Keystore
func (mk *MemKeystore) Delete(name string) error {
	if err := validateName(name); err != nil {
		return err
	}

	delete(mk.keys, name)
	return nil
}

// List return a list of key identifier
func (mk *MemKeystore) List() ([]string, error) {
	out := make([]string, 0, len(mk.keys))
	for k, _ := range mk.keys {
		out = append(out, k)
	}
	return out, nil
}
