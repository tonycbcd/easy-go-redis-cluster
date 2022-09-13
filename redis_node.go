// Copyright (C) 2022
// Author FrankXu <frankxury@gmail.com>
// Build on 2022/07/12

// The redis node.

package redis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	//goredis "github.com/go-redis/redis/v8"
)

type slotArea struct {
	StartSlot uint16
	EndSlot   uint16
}

type redisNode struct {
	Id        string
	Ip        string
	Port      string
	Role      string // support: master or slave.
	MasterId  string // for slave role.
	SlotAreas []*slotArea
	SlotName  string
}

const (
	ROLE_MASTER = "master"
	ROLE_SLAVE  = "slave"

	FIXED_SLOT_KEY = "{%s}:%s"
)

// for examples:
// 25837095b1df96c37ffa96493e4bf2e693630be7 172.29.16.7:6379@1122 master - 0 1663066583000 1 connected 8192-11406 14138-16383
// 7bc86a205acc548ffe415dc6649f636a273d655f 172.29.18.7:6379@1122 master - 0 1663066583895 2 connected 5462-8191 11407-14137
// 8f3428825dcddfd603ad07bb6219fc756efc7102 172.29.19.4:6379@1122 myself,master - 0 1663066579000 0 connected 0-5461
func NewRedisNode(info string) (*redisNode, error) {
	newNode := &redisNode{}
	err := newNode.ParseAndSet(info)
	return newNode, err
}

func (this *redisNode) ParseAndSet(info string) error {
	comps := strings.Split(info, " - ")
	compsLen := len(comps)
	if compsLen == 2 {
		return this.parseAndSetMaster(comps[0], comps[1])
	}
	return this.parseAndSetSlave(info)
}

func (this *redisNode) parseAndSetMaster(frontInfo, lastInfo string) error {
	// to parse the frontInfo.
	comps := strings.Split(frontInfo, " ")
	compsLen := len(comps)
	if compsLen < 3 || strings.Index(comps[2], ROLE_MASTER) < 0 {
		return errors.New("info error")
	}

	this.Id = comps[0]
	this.Ip, this.Port = this.getAddr(comps[1])
	this.Role = ROLE_MASTER

	// to parse the lastInfo.
	comps = strings.Split(lastInfo, " ")
	for _, one := range comps {
		if strings.Index(one, "-") > 0 {
			startSlot, endSlot := this.getSlots(one)
			this.SlotAreas = append(this.SlotAreas, &slotArea{startSlot, endSlot})
			if this.SlotName == "" && endSlot > 0 {
				this.SlotName = NewCRC16().GetHashBySlotArea("n", startSlot, endSlot)
				break
			}
		}
	}

	return nil
}

func (this *redisNode) getSlots(info string) (uint16, uint16) {
	comps := strings.Split(info, "-")
	if len(comps) != 2 {
		return 0, 0
	}
	startSlot, _ := strconv.ParseUint(comps[0], 10, 64)
	endSlot, _ := strconv.ParseUint(comps[1], 10, 64)

	return uint16(startSlot), uint16(endSlot)
}

func (this *redisNode) getAddr(info string) (string, string) {
	comps := strings.Split(info, "@")
	if len(comps) == 2 {
		info = comps[0]
	}
	comps = strings.Split(info, ":")
	if len(comps) != 2 {
		return "", ""
	}

	return comps[0], comps[1]
}

func (this *redisNode) parseAndSetSlave(info string) error {
	comps := strings.Split(info, "slave")
	compsLen := len(comps)
	if compsLen != 2 {
		return errors.New("not slave node")
	}

	frontInfo := comps[0]
	lastInfo := comps[1]

	// to parse the frontInfo.
	comps = strings.Split(frontInfo, " ")
	if len(comps) < 2 {
		return errors.New("slave info error")
	}
	this.Id = comps[0]
	this.Ip, this.Port = this.getAddr(comps[1])
	this.Role = ROLE_SLAVE

	// to parse the lastInfo.
	comps = strings.Split(lastInfo, " ")
	for _, one := range comps {
		if one != "" {
			this.MasterId = one
			break
		}
	}

	return nil
}

func (this *redisNode) RebuildKey(key string) string {
	return fmt.Sprintf(FIXED_SLOT_KEY, this.SlotName, key)
}
