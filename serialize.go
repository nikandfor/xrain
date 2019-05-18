package xrain

import (
	"fmt"
	"log"
)

type (
	SerializeContext struct {
		Back     Back
		Page     int64
		Freelist Freelist
		Name     string
	}

	Serializer interface {
		SerializerName() string
		Serialize(p []byte) int
		Deserialize(ctx *SerializeContext, p []byte) (interface{}, int, error)
	}
)

var Serializers = map[string]Serializer{}

func init() {
	for _, o := range []Serializer{
		&Freelist2{},
		&FileTree{},
		&FixedLayout{},
	} {
		Serializers[o.SerializerName()] = o
	}
}

func Serialize(p []byte, o Serializer) int {
	n := o.SerializerName()
	if len(n) > 256 {
		log.Fatalf("too long serializer name: %v", n)
	}
	s := 0
	p[s] = byte(len(n))
	s++
	s += copy(p[s:], n)
	s += o.Serialize(p[s:])
	return s
}

func Deserialize(ctx *SerializeContext, p []byte) (interface{}, int, error) {
	s := 0
	nl := int(p[s])
	s++
	name := string(p[s : s+nl])
	s += nl
	o := Serializers[name]
	if o == nil {
		return nil, s, fmt.Errorf("no such serializer: %v", name)
	}
	ctx.Name = name
	r, ss, err := o.Deserialize(ctx, p[s:])
	return r, s + ss, err
}
