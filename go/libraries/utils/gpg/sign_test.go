// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gpg

import (
	"context"
	"encoding/pem"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSign(t *testing.T) {
	ctx := context.Background()
	keyId := "4798B29CA9029452D103B1E388F64CE29826DA4A"

	message := []byte("I did a thing")
	signature, err := Sign(ctx, keyId, message)
	require.NoError(t, err)
	require.NotNil(t, signature)
	require.NotEmpty(t, signature)
}

func TestDecodeAllPemBlocks(t *testing.T) {
	pemBlock := `
-----BEGIN PGP SIGNED MESSAGE-----
Hash: SHA256

I did a thing
-----BEGIN PGP SIGNATURE-----

iQIzBAEBCAAdFiEER5iynKkClFLRA7HjiPZM4pgm2koFAmbH0PIACgkQiPZM4pgm
2kpiyRAAjyMs8dkydx87w2N8aM7skbOzfk7Xhg+NJYS+MVk465EHSfW5WQZmXdbu
jupH3FPmB1UiiGTCP7Igrhl/KU9WrEXLvrYBI07aHly0bRiy6COd6E/+YIEz0uaW
nfbcHgR9zkVxl5lj42HReGUnkbK9KYbu32AZznu8F5/qlHOA7e06ILivyKrRXkRT
vbC/IaW0pRRJqTDylAEFEDsIYP2pFhIgIE/aEoid6Bb93yh1jmZTci6aOWd0B5o7
hLWFJAmyFSiMAi+WrWnKAD9xeq5gGJjpyWNW7XpeIQ7E/xUeLeHvvrgnOd9O5w8n
hFqz7IV6EMXc6dFz/sivusnb0viTUS2fsqsS2k4vA/1SyoE5ffTM/BFpWBCevAmi
og0VVLur5OcijL+hyGXuuOVSgKkiuu6KKjg/Qer3/iJB5KYIlDoZIjSevLP2O6ZD
FM/wBFp6aBIhh/dl/1DqW6F0HM23J5dIV5SAf7uLPHN7nKky6XcFiILRrET1uC6R
j9KK38Dhaa4o30uf5RZsSy03hP/mUCS+U5NVREnN12Z3RETSkarfZ3wgiV2ftMup
z22q4wvtUGB5whOZ5D3PW7df5LREGbxfKvn159a3OccEKa3UURWDF3V63apV2OMN
YN6Sszg+o8Aw0AT4M6nrLTe3YaIE6sR4YMxOCSOPAT9oSDg1t5s=
=fklH
-----END PGP SIGNATURE-----`

	pemBlocks, err := DecodeAllPEMBlocks([]byte(pemBlock))
	require.NoError(t, err)
	fmt.Println(len(pemBlocks))
	require.Len(t, pemBlocks, 2)
	require.True(t, containsBlockOfType(pemBlocks, "PGP SIGNED MESSAGE"))
	require.True(t, containsBlockOfType(pemBlocks, "PGP SIGNATURE"))

	output, err := Verify(context.Background(), []byte(pemBlock))
	require.NoError(t, err)
	require.NotNil(t, output)
}

func containsBlockOfType(blocks []*pem.Block, blockType string) bool {
	for _, block := range blocks {
		if block.Type == blockType {
			return true
		}
	}
	return false
}
