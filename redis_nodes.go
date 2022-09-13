// Copyright (C) 2022
// Author FrankXu <frankxury@gmail.com>
// Build on 2022/07/12

// The redis nodes.

package redis

import (
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

// 25837095b1df96c37ffa96493e4bf2e693630be7 172.29.16.7:6379@1122 master - 0 1663066583000 1 connected 8192-11406 14138-16383\n7bc86a205acc548ffe415dc6649f636a273d655f 172.29.18.7:6379@1122 master - 0 1663066583895 2 connected 5462-8191 11407-14137\n8f3428825dcddfd603ad07bb6219fc756efc7102 172.29.19.4:6379@1122 myself,master - 0 1663066579000 0 connected 0-5461\n
func NewRedisNodes(info string) (*redisNodes, error) {
	if oneRedisNodes == nil {
		oneRedisNodes = &redisNodes{groupMap: map[string]*redisGroup{}}
	}
	err := oneRedisNodes.ParseAndSet(info)
	return oneRedisNodes, err
}

func (this *redisNodes) ParseAndSet(info string) error {
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
		for _, slotArea := range node.master.SlotAreas {
			if crc16Val >= slotArea.StartSlot && crc16Val <= slotArea.EndSlot {
				isFound = true
				hitNode = node
				break
			}
		}

		if isFound {
			break
		}
	}

	return hitNode, isFound
}
