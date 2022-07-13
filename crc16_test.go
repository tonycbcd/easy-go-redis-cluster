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
		{"relationsv2:nmm:128474726:only_like", 57134},
		{"relationsv2:nmm:128474726:block", 10540},
		{"search:prof:nmm:128474726:locationAndVideo", 38429},
		{"search:gr:list:sdm:70817206", 477},
		{"search:user:condition:sdm:70817206", 46806},
	}

	assert := assert.New(t)
	crc16 := NewCRC16()
	for _, test := range testCases {
		hashId := crc16.Encode(test.Key)
		fmt.Printf("%s, %d\n", test.Key, hashId)
		assert.Equal(hashId, test.Res, "test failed.")
	}
}
