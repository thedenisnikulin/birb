package key

import (
	bval "birb/bvalue"
	"birb/txid"
	"errors"
	"strings"

	"github.com/samber/mo"
)

type Key struct {
	Type       string
	Ns         string
	FieldName  string
	FieldValue bval.Value
	TxState    string
	Xmin       txid.ID
	Xmax       txid.ID
}

func (k Key) String() string {
	return k.Type + "_" + k.Ns + "_" + k.FieldName + "_" +
		k.FieldValue.String() + "_" + k.TxState + "_" +
		k.Xmin.String() + "_" + k.Xmax.String()
}

func New(ktype, ns, field string, value bval.Value, txstate string, xmin txid.ID, xmax mo.Option[txid.ID]) Key {
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

func Index(ns, field string, value bval.Value, txstate string, xmin txid.ID, xmax mo.Option[txid.ID]) Key {
	return New("idx", ns, field, value, txstate, xmin, xmax)
}

func Record(ns, field string, value bval.Value, txstate string, xmin txid.ID, xmax mo.Option[txid.ID]) Key {
	return New("rec", ns, field, value, txstate, xmin, xmax)
}

func Pointer(ns, field string, value bval.Value, txstate string, xmin txid.ID, xmax mo.Option[txid.ID]) Key {
	return New("ptr", ns, field, value, txstate, xmin, xmax)
}

func FromString(s string) (Key, error) {
	tokens := strings.Split(s, "_")
	if len(tokens) != 7 {
		return Key{}, errors.New("string is not a valid key")
	}

	xmin, err := txid.FromString(tokens[5])
	if err != nil {
		return Key{}, err
	}

	xmax, err := txid.FromString(tokens[6])
	if err != nil {
		return Key{}, err
	}

	return New(
		tokens[0], tokens[1],
		tokens[2], bval.FromString(tokens[3]),
		tokens[4], xmin, mo.Some(xmax),
	), nil
}
