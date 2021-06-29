package canal

import (
	"crypto/sha1"
)

func Scramble411(data []byte, seed []byte) []byte {
	crypt := sha1.New()
	crypt.Write(data)
	pass1 := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(pass1)
	pass2 := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(seed)
	crypt.Write(pass2)
	pass3 := crypt.Sum(nil)
	for i := range pass3 {
		pass3[i] ^= pass1[i]
	}
	return pass3
}
