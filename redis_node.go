// Copyright 2022, SuccessfulMatch.com All rights reserved.
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

type redisNode struct {
	Id        string
	Ip        string
	Port      string
	Role      string // support: master or slave.
	MasterId  string // for slave role.
	StartSlot uint16
	EndSlot   uint16
	SlotName  string
}

const (
	ROLE_MASTER = "master"
	ROLE_SLAVE  = "slave"

	FIXED_SLOT_KEY = "{%s}:%s"
)

// for examples:
//  2be788e052541b5184fd86d74031eefb85814796 172.17.0.1:8001@18001 myself,master - 0 1657610133000 1 connected 0-5460
//  8b8d15c52ef32935ea5561eeb6698f9ebf062462 172.24.0.1:8005@18005 slave 90f04f9df96bd416194a548a64cc4d282764e2d2 0 1657610134793 5 connected
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
			this.StartSlot, this.EndSlot = this.getSlots(one)
			if this.EndSlot > 0 {
				this.SlotName = NewCRC16().GetHashBySlotArea("n", this.StartSlot, this.EndSlot)
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
