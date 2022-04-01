package tendermint

import (
	"fmt"

	"github.com/tendermint/tendermint/crypto"
	"github.com/tendermint/tendermint/crypto/ed25519"
	"github.com/tendermint/tendermint/crypto/secp256k1"
	"github.com/tendermint/tendermint/crypto/sr25519"

	"github.com/daotl/go-acei/types"
)

/* -- Tendermint -- */

func Ed25519ValidatorUpdate(pk []byte, power int64) ValidatorUpdate {
	pke := ed25519.PubKey(pk)

	pkp, err := PubKeyToProto(pke)
	if err != nil {
		panic(err)
	}

	return ValidatorUpdate{
		PubKey: pkp,
		Power:  power,
	}
}

func UpdateValidator(pk []byte, power int64, keyType string) ValidatorUpdate {
	switch keyType {
	case "", ed25519.KeyType:
		return Ed25519ValidatorUpdate(pk, power)
	case secp256k1.KeyType:
		pke := secp256k1.PubKey(pk)
		pkp, err := PubKeyToProto(pke)
		if err != nil {
			panic(err)
		}
		return ValidatorUpdate{
			PubKey: pkp,
			Power:  power,
		}
	case sr25519.KeyType:
		pke := sr25519.PubKey(pk)
		pkp, err := PubKeyToProto(pke)
		if err != nil {
			panic(err)
		}
		return ValidatorUpdate{
			PubKey: pkp,
			Power:  power,
		}
	default:
		panic(fmt.Sprintf("key type %s not supported", keyType))
	}
}

// From: https://github.com/tendermint/tendermint/blob/8441b3715aff9dbfcb9cbe29ebc2f53e7cd910d3/crypto/encoding/codec.go#L20
// PubKeyToProto takes PubKey and transforms it to a protobuf Pubkey
func PubKeyToProto(k crypto.PubKey) (types.PublicKey, error) {
	var kp types.PublicKey
	switch k := k.(type) {
	case ed25519.PubKey:
		kp = types.PublicKey{
			Sum: &types.PublicKey_Ed25519{
				Ed25519: k,
			},
		}
	case secp256k1.PubKey:
		kp = types.PublicKey{
			Sum: &types.PublicKey_Secp256K1{
				Secp256K1: k,
			},
		}
	case sr25519.PubKey:
		kp = types.PublicKey{
			Sum: &types.PublicKey_Sr25519{
				Sr25519: k,
			},
		}
	default:
		return kp, fmt.Errorf("toproto: key type %v is not supported", k)
	}
	return kp, nil
}
