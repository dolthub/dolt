go-crypto contains the following packages:

* **drbg**: a cryptographically secure pseudorandom number generator
* **encoding/base32**: a compact base32 encoder
* **pwclip**: the password derivation algorithm used in [pwclip](https://github.com/davidlazar/pwclip)
* **secretkey**: user-friendly secret keys that can be used with secretbox

It also includes packages that you
[probably don't need](https://www.imperialviolet.org/2014/06/27/streamingencryption.html):

* **salsa20**: a streaming interface (`cipher.Stream`) for the Salsa20 stream cipher
* **poly1305**: a streaming interface (`hash.Hash`) for the Poly1305 one-time authenticator
