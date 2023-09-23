package key

import (
	bval "birb/bvalue"
	"birb/txid"
	"errors"
	"strings"

	"github.com/samber/mo"
)

// Key for accessing committed data in storage.
// Key layout pattern:
// {key type}_{tx state}_{ns}_{idx field name}_{idx field val}_{xmin val}_{xmax val},
// Example:
// rec_com_users_pk_12_1234567890_9876543210.
type Key struct {
	// type of value stored ('rec', 'ptr', 'idx')
	Type string
	// namespace
	Ns string
	// name of indexed field (arbitrary or 'pk')
	FieldName string
	// value of indexed field (e.g. pk's value)
	FieldValue bval.Value
	// state of row's associated tx ('unc' or 'com')
	TxState string
	// starting from this txid the key is discoverable
	Xmin txid.ID
	// starting from this txid the key is undiscoverable
	Xmax txid.ID
}

func (k Key) ToUnc() UncKey {
	return UncRec(k.Ns, k.FieldName, k.FieldValue, k.Xmin, mo.Some(k.Xmax))
}

func (k Key) String() string {
	return k.Type + "_" + k.TxState + "_" + k.Ns + "_" +
		k.FieldName + "_" + k.FieldValue.String() + "_" +
		k.Xmin.String() + "_" + k.Xmax.String()
}

// Key for accessing uncommitted data in storage.
// Key layout pattern:
// {key type}_{tx state}_{xmin val}_{xmax val}_{ns}_{idx field name}_{idx field val},
// Example:
// rec_unc_1234567890_9876543210_users_pk_12.
type UncKey struct {
	Key
}

func (k UncKey) ToCom() Key {
	return ComRec(k.Ns, k.FieldName, k.FieldValue, k.Xmin, mo.Some(k.Xmax))
}

func (k UncKey) String() string {
	return k.Type + "_" + k.TxState + "_" +
		k.Xmin.String() + "_" + k.Xmax.String() + "_" +
		k.Ns + "_" + k.FieldName + "_" + k.FieldValue.String()
}

// Prefix for finding keys of uncommitted data within same transaction.
func PrefixUncSameTx(ktype string, ns string, xmin txid.ID, xmax mo.Option[txid.ID]) string {
	xmaxMust := txid.FromUint64(0)
	if xmax.IsPresent() {
		xmaxMust = xmax.MustGet()
	}

	return ktype + "_unc_" + xmin.String() + "_" + xmaxMust.String() + "_" + ns
}

func new(ktype, ns, field string, value bval.Value, txstate string, xmin txid.ID, xmax mo.Option[txid.ID]) Key {
	xmaxMust := txid.FromUint64(0)
	if xmax.IsPresent() {
		xmaxMust = xmax.MustGet()
	}

	return Key{
		Type:       ktype,
		Ns:         ns,
		FieldName:  field,
		FieldValue: value,
		TxState:    txstate,
		Xmin:       xmin,
		Xmax:       xmaxMust,
	}
}

// TODO INDEXES HERE
// for committed indexes, just use ptrs [idx_com_users_name_den] (notice no tx info (or need it?)) = [rec_com_users_pk_12_1_1]
// for uncommitted indexes, use also ptrs but uncommitted [idx_unc_1_1_users_name_den] = [rec_unc_1_1_users_pk_12]
// uncommitted indexes are FULLY IGNORED AND NOT KNOWN for collection.Store, they are only updated and used by tx.Store
// может нахуй без индексов сделать tx.Store? нахуй они нужны вообще

func newUnc(ktype, ns, field string, value bval.Value, txstate string, xmin txid.ID, xmax mo.Option[txid.ID]) UncKey {
	return UncKey{new(ktype, ns, field, value, txstate, xmin, xmax)}
}

func Idx(ns, field string, value bval.Value, txstate string, xmin txid.ID, xmax mo.Option[txid.ID]) Key {
	return new("idx", ns, field, value, txstate, xmin, xmax)
}

func Ptr(ns, field string, value bval.Value, txstate string, xmin txid.ID, xmax mo.Option[txid.ID]) Key {
	return new("ptr", ns, field, value, txstate, xmin, xmax)
}

func ComRec(ns, field string, value bval.Value, xmin txid.ID, xmax mo.Option[txid.ID]) Key {
	return new("rec", ns, field, value, "com", xmin, xmax)
}

func UncRec(ns, field string, value bval.Value, xmin txid.ID, xmax mo.Option[txid.ID]) UncKey {
	return newUnc("rec", ns, field, value, "unc", xmin, xmax)
}

func FromString(s string) (Key, error) {
	tokens := strings.Split(s, "_")
	if len(tokens) != 7 {
		return Key{}, errors.New("provided string is not a valid key")
	}

	switch tokens[0] {
	case "rec":
		xmin, err := txid.FromString(tokens[5])
		if err != nil {
			return Key{}, err
		}

		xmax, err := txid.FromString(tokens[6])
		if err != nil {
			return Key{}, err
		}

		return new(
			tokens[0], tokens[2],
			tokens[3], bval.FromString(tokens[4]),
			"com", xmin, mo.Some(xmax),
		), nil
	case "idx":
		panic("FromString for idx keys is not implemented")
	}

	panic("how are u even here")
}

func FromStringUnc(s string) (UncKey, error) {
	tokens := strings.Split(s, "_")
	if len(tokens) != 7 {
		return UncKey{}, errors.New("provided string is not a valid key")
	}

	switch tokens[0] {
	case "rec":
		xmin, err := txid.FromString(tokens[2])
		if err != nil {
			return UncKey{}, err
		}

		xmax, err := txid.FromString(tokens[3])
		if err != nil {
			return UncKey{}, err
		}

		return newUnc(
			tokens[0], tokens[4],
			tokens[5], bval.FromString(tokens[6]),
			"unc", xmin, mo.Some(xmax),
		), nil
	case "idx":
		panic("FromStringUnc for idx keys is not implemented")
	}

	panic("how are u even here")
}
