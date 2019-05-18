package xrain

import (
	"log"
	"testing"
)

func TestSerialize(t *testing.T) {
	for _, s := range []string{"xrain.Freelist2", "xrainlong.TreeFreelist", "xrain.FixedLayout"} {
		log.Printf("%2x - %s", len(s), s)
	}
}
