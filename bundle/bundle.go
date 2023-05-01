package bundle

import (
	rc "MxcMQ/registrationCenter"
	"errors"
	"hash/crc32"
	"sync"
)

type Bundles struct {
	Bundles map[int]*Bundle
}

type Bundle struct {
	Info       rc.BundleNode
	Partitions sync.Map
	// pnode map[string]rc.PartitionNode
}

type BundleInfo struct {
}

var (
	defaultNumberOfBundles = 16
	MaxAddress             = 0xFFFFFFFF
)

func NewBundles() (*Bundles, error) {
	bs := &Bundles{
		Bundles: make(map[int]*Bundle),
	}
	for i := 1; i <= defaultNumberOfBundles; i++ {
		b, err := NewBundle(i)
		if err != nil {
			return nil, err
		}
		bs.Bundles[i] = b
	}

	return bs, nil
}

func NewBundle(id int) (*Bundle, error) {
	shard := MaxAddress / defaultNumberOfBundles
	uint32Shard := uint32(shard)
	info := rc.BundleNode{
		ID:    id,
		End:   uint32Shard * uint32(id),
		Start: uint32Shard*uint32(id) - uint32Shard,
	}
	b := &Bundle{Info: info}
	return b, nil
}

func (bs *Bundles) GetBundle(topic string) (int, error) {
	address := crc32.ChecksumIEEE([]byte(topic))
	return bs.bsearch(address)
}

func (bs *Bundles) bsearch(key uint32) (int, error) {
	if key == uint32(MaxAddress) {
		return 1, nil
	}

	bnum := len(bs.Bundles)
	left := 0
	right := bnum - 1
	for left <= right {
		mid := bs.Bundles[(left+right)/2]
		if key > mid.Info.End-1 {
			left = mid.Info.ID
		} else if key < mid.Info.Start {
			right = mid.Info.ID - 1
		} else {
			return mid.Info.ID, nil
		}
	}

	return 0, errors.New("not found")
}
