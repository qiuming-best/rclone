package crypto

import (
	"encoding/base64"
	"testing"
)

func TestDecodeCrash(t *testing.T) {
	// key can be anything
	key := Key{}

	// Create a bit of data that is the size of the RSA key (128 bytes for 1024 bit key)
	// We want rsaDecrypt to fail (return nil) or return short data.
	// If we pass random data, it is highly likely that rsaDecrypt returns nil
	// because it won't find the expected padding (0x00 byte).
	// The panic happens when rsaDecrypt returns nil (len 0) or short data < 16 bytes.

	// 128 bytes of A
	data := make([]byte, 128)
	for i := range data {
		data[i] = 'A'
	}
	encoded := base64.StdEncoding.EncodeToString(data)

	// This should NOT panic
	_, err := Decode(encoded, key)
	if err == nil {
		// It might return error or nil, but SHOULD NOT PANIC
		// If it accepts garbage, that's also "safe" regarding panic, but likely it should error.
	} else {
		t.Logf("Got expected error: %v", err)
	}
}
