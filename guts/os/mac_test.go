package os_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	gos "github.com/daotl/guts/os"
)

func TestMAC(t *testing.T) {
	mac, err := gos.GetMACStr(true)
	require.NoError(t, err)
	fmt.Println("MAC: " + mac)

	macu, err := gos.GetMACUint64(true)
	require.NoError(t, err)
	fmt.Printf("MAC represented in uint64: %16.16X\n", macu)
}
