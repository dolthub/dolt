package options

const (
	RSAKey     = "rsa"
	Ed25519Key = "ed25519"

	DefaultRSALen = 2048
)

type KeyGenerateSettings struct {
	Algorithm string
	Size      int
}

type KeyRenameSettings struct {
	Force bool
}

type KeyGenerateOption func(*KeyGenerateSettings) error
type KeyRenameOption func(*KeyRenameSettings) error

func KeyGenerateOptions(opts ...KeyGenerateOption) (*KeyGenerateSettings, error) {
	options := &KeyGenerateSettings{
		Algorithm: RSAKey,
		Size:      -1,
	}

	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return nil, err
		}
	}
	return options, nil
}

func KeyRenameOptions(opts ...KeyRenameOption) (*KeyRenameSettings, error) {
	options := &KeyRenameSettings{
		Force: false,
	}

	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return nil, err
		}
	}
	return options, nil
}

type keyOpts struct{}

var Key keyOpts

// Type is an option for Key.Generate which specifies which algorithm
// should be used for the key. Default is options.RSAKey
//
// Supported key types:
// * options.RSAKey
// * options.Ed25519Key
func (keyOpts) Type(algorithm string) KeyGenerateOption {
	return func(settings *KeyGenerateSettings) error {
		settings.Algorithm = algorithm
		return nil
	}
}

// Size is an option for Key.Generate which specifies the size of the key to
// generated. Default is -1
//
// value of -1 means 'use default size for key type':
//  * 2048 for RSA
func (keyOpts) Size(size int) KeyGenerateOption {
	return func(settings *KeyGenerateSettings) error {
		settings.Size = size
		return nil
	}
}

// Force is an option for Key.Rename which specifies whether to allow to
// replace existing keys.
func (keyOpts) Force(force bool) KeyRenameOption {
	return func(settings *KeyRenameSettings) error {
		settings.Force = force
		return nil
	}
}
