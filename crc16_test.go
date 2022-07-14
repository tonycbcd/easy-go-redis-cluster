/// Copyright 2022, SuccessfulMatch.com All rights reserved.
// Author FrankXu <frankxury@gmail.com>
// Build on 2022/07/11

// The redis crc16 test.

package redis

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHashSlot(t *testing.T) {
	testCases := []struct {
		Key string
		Res uint16
	}{
		{"relationsv2:nmm:128474726:only_like", 7982},
		{"relationsv2:nmm:128474726:block", 10540},
		{"search:prof:nmm:128474726:locationAndVideo", 5661},
		{"search:gr:list:sdm:70817206", 477},
		{"search:user:condition:sdm:70817206", 14038},
		{"566748c8b1b2bb6b127f51be704c13b5aab701bc", 13821},
	}

	assert := assert.New(t)
	crc16 := NewCRC16()
	for _, test := range testCases {
		hashId := crc16.HashSlot(test.Key)
		fmt.Printf("%s, %d\n", test.Key, hashId)
		assert.Equal(hashId, test.Res, "test failed.")
	}
}

func TestHashSlotCore(t *testing.T) {
	testCases := []struct {
		Key string
		Num uint16
		Res uint16
	}{
		{"-65722", 3, 2},
		{"1657765722", 3, 2},
		{"1657765723", 3, 2},
		{"1657765724", 3, 0},
		{"1657765725", 3, 0},
		{"1657765726", 3, 2},
		{"1657765727", 3, 2},
		{"1657765728", 3, 0},
		{"1657765729", 3, 0},
		{"1657765730", 3, 0},
		{"1657765731", 3, 0},
		{"1657765732", 3, 2},
	}

	assert := assert.New(t)
	crc16 := NewCRC16()
	for _, test := range testCases {
		hashId := crc16.HashSlotCore(test.Key, test.Num)
		fmt.Printf("%s, %d\n", test.Key, hashId)
		assert.Equal(hashId, test.Res, "test failed.")
	}
}

func TestGetHashBySlotArea(t *testing.T) {
	testCases := []struct {
		Key string
		Sta uint16
		End uint16
		Res string
	}{
		{"n", 0, 8191, "nB"},
		{"n", 8192, 16383, "nA"},
		{"node", 0, 5460, "nodeB"},
		{"node", 5461, 10922, "nodeD"},
		{"node", 10923, 16383, "nodeA"},
		{"node", 14745, 16383, "nodeDZ"},
	}

	assert := assert.New(t)
	crc16 := NewCRC16()
	for _, test := range testCases {
		hash := crc16.GetHashBySlotArea(test.Key, test.Sta, test.End)
		fmt.Printf("%#v, %s\n", test, hash)
		assert.Equal(hash, test.Res, "test failed.")
	}
}
