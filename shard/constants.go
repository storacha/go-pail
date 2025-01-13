package shard

// KeyCharsASCII refers to printable ASCII characters in the code range 32-126.
const KeyCharsASCII = "ascii"

// MaxKeySize is a default maximum key size in bytes. It is the same as MAX_PATH
// - the maximum filename+path size on most windows/unix systems, so should be
// sufficient for most purposes.
const MaxKeySize = 4096
