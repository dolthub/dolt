package corehttp

// TODO: move to IPNS
const WebUIPath = "/ipfs/QmPhnvn747LqwPYMJmQVorMaGbMSgA7mRRoyyZYz3DoZRQ"

// this is a list of all past webUI paths.
var WebUIPaths = []string{
	WebUIPath,
	"/ipfs/QmXX7YRpU7nNBKfw75VG7Y1c3GwpSAGHRev67XVPgZFv9R",
	"/ipfs/QmXdu7HWdV6CUaUabd9q2ZeA4iHZLVyDRj3Gi4dsJsWjbr",
	"/ipfs/QmaaqrHyAQm7gALkRW8DcfGX3u8q9rWKnxEMmf7m9z515w",
	"/ipfs/QmSHDxWsMPuJQKWmVA1rB5a3NX2Eme5fPqNb63qwaqiqSp",
	"/ipfs/QmctngrQAt9fjpQUZr7Bx3BsXUcif52eZGTizWhvcShsjz",
	"/ipfs/QmS2HL9v5YeKgQkkWMvs1EMnFtUowTEdFfSSeMT4pos1e6",
	"/ipfs/QmR9MzChjp1MdFWik7NjEjqKQMzVmBkdK3dz14A6B5Cupm",
	"/ipfs/QmRyWyKWmphamkMRnJVjUTzSFSAAZowYP4rnbgnfMXC9Mr",
	"/ipfs/QmU3o9bvfenhTKhxUakbYrLDnZU7HezAVxPM6Ehjw9Xjqy",
}

var WebUIOption = RedirectOption("webui", WebUIPath)
