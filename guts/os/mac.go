package os

import (
	"bytes"
	"errors"
	"net"
)

var ErrMACInterfaceNotFound = errors.New("MAC interface not found")

// GetMACInterface returns the network interface associated with the MAC address of the host machine.
func GetMACInterface(includeLocal bool) (iface *net.Interface, err error) {
	var ifs []net.Interface
	if ifs, err = net.Interfaces(); err != nil {
		return nil, err
	}

	for _, i := range ifs {
		if i.Flags&net.FlagUp != 0 && bytes.Compare(i.HardwareAddr, nil) != 0 &&
			// Skip locally administered addresses (b1==1 in OUI)
			// See: https://www.wikiwand.com/en/MAC_address#/Address_details
			i.HardwareAddr[0]&2 == 0 {
			return &i, nil
		}
	}

	// Fallback to locally administered addresses
	if includeLocal {
		for _, i := range ifs {
			if i.Flags&net.FlagUp != 0 && bytes.Compare(i.HardwareAddr, nil) != 0 {
				return &i, nil
			}
		}
	}

	return nil, ErrMACInterfaceNotFound
}

// GetMACStr returns the MAC address of the host machine.
func GetMACStr(includeLocal bool) (string, error) {
	if i, err := GetMACInterface(includeLocal); err != nil {
		return "", err
	} else {
		return i.HardwareAddr.String(), nil
	}
}

// GetMACStr returns the MAC address of the host machine represented in uint64.
//
// Reference: https://gist.github.com/tsilvers/085c5f39430ced605d970094edf167ba
func GetMACUint64(includeLocal bool) (uint64, error) {
	if i, err := GetMACInterface(includeLocal); err != nil {
		return 0, err
	} else {
		var mac uint64
		for j, b := range i.HardwareAddr {
			if j >= 8 {
				break
			}
			mac <<= 8
			mac += uint64(b)
		}
		return mac, nil
	}
}
