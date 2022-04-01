package encoding

import (
	"fmt"

	"github.com/tendermint/tendermint/crypto"
	"github.com/tendermint/tendermint/crypto/ed25519"
	"github.com/tendermint/tendermint/crypto/secp256k1"
	"github.com/tendermint/tendermint/crypto/sr25519"

	"github.com/daotl/go-acei/types"
)

// From: https://github.com/tendermint/tendermint/blob/9cee35bb8caed44c9f9af0a50f3d9a32454ebe76/crypto/encoding/codec.go

// PubKeyFromProto takes a protobuf Pubkey and transforms it to a crypto.Pubkey
func PubKeyFromProto(k types.PublicKey) (crypto.PubKey, error) {
	switch k := k.Sum.(type) {
	case *types.PublicKey_Ed25519:
		if len(k.Ed25519) != ed25519.PubKeySize {
			return nil, fmt.Errorf("invalid size for PubKeyEd25519. Got %d, expected %d",
				len(k.Ed25519), ed25519.PubKeySize)
		}
		pk := make(ed25519.PubKey, ed25519.PubKeySize)
		copy(pk, k.Ed25519)
		return pk, nil
	case *types.PublicKey_Secp256K1:
		if len(k.Secp256K1) != secp256k1.PubKeySize {
			return nil, fmt.Errorf("invalid size for PubKeySecp256k1. Got %d, expected %d",
				len(k.Secp256K1), secp256k1.PubKeySize)
		}
		pk := make(secp256k1.PubKey, secp256k1.PubKeySize)
		copy(pk, k.Secp256K1)
		return pk, nil
	case *types.PublicKey_Sr25519:
		if len(k.Sr25519) != sr25519.PubKeySize {
			return nil, fmt.Errorf("invalid size for PubKeySr25519. Got %d, expected %d",
				len(k.Sr25519), sr25519.PubKeySize)
		}
		pk := make(sr25519.PubKey, sr25519.PubKeySize)
		copy(pk, k.Sr25519)
		return pk, nil
	default:
		return nil, fmt.Errorf("fromproto: key type %v is not supported", k)
	}
}
