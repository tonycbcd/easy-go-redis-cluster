// Copyright (C) 2022
// Author FrankXu <frankxury@gmail.com>
// Build on 2022/07/12

// The redis nodes.

package redis

import (
	"fmt"
	"strings"
	"sync"
)

type redisGroup struct {
	master *redisNode
	slaves []*redisNode
}

type redisNodes struct {
	groupMap map[string]*redisGroup
	lock     sync.Mutex
}

var (
	oneRedisNodes *redisNodes
)

func NewRedisNodes(info string) (*redisNodes, error) {
	if oneRedisNodes == nil {
		oneRedisNodes = &redisNodes{groupMap: map[string]*redisGroup{}}
	}
	err := oneRedisNodes.ParseAndSet(info)
	return oneRedisNodes, err
}

func (this *redisNodes) ParseAndSet(info string) error {
	fmt.Printf("Redis Info: %#v\n", info)
	comps := strings.Split(info, "\n")
	newMap := map[string]*redisGroup{}
	for _, nodeInfo := range comps {
		if nodeInfo == "" {
			continue
		}
		if node, err := NewRedisNode(nodeInfo); err == nil {
			switch node.Role {
			case ROLE_MASTER:
				if _, isExists := newMap[node.Id]; !isExists {
					newMap[node.Id] = &redisGroup{slaves: []*redisNode{}}
				}
				newMap[node.Id].master = node
			case ROLE_SLAVE:
				if _, isExists := newMap[node.MasterId]; !isExists {
					newMap[node.MasterId] = &redisGroup{slaves: []*redisNode{}}
				}
				newMap[node.MasterId].slaves = append(newMap[node.MasterId].slaves, node)
			}
		} else {
			return err
		}
	}

	this.lock.Lock()
	this.groupMap = newMap
	this.lock.Unlock()

	return nil
}

func (this *redisNodes) FindNodeByCRC16Val(crc16Val uint16) (*redisGroup, bool) {
	var hitNode *redisGroup
	isFound := false
	for _, node := range this.groupMap {
		if crc16Val >= node.master.StartSlot && crc16Val <= node.master.EndSlot {
			isFound = true
			hitNode = node
		}
	}

	return hitNode, isFound
}
