// Copyright (C) 2022
// Author FrankXu <frankxury@gmail.com>
// Build on 2022/07/13

// The redis client.

package redis

import (
	"context"
	"errors"
	"fmt"
	goredis "github.com/go-redis/redis/v8"
	"math/rand"
	"sync"
	"time"
)

type RedisClientFactory struct {
	store   sync.Map
	options *goredis.ClusterOptions
}

type RedisClient struct {
	*goredis.Client
}

var (
	oneRedisClientFactory *RedisClientFactory
)

func NewRedisClient(op *goredis.Options) (*RedisClient, error) {
	r := goredis.NewClient(op)
	oneRedisClient := &RedisClient{r}
	if _, err := oneRedisClient.Ping(context.Background()).Result(); err != nil {
		return nil, err
	}
	return oneRedisClient, nil
}

func NewRedisClientFactory(op *goredis.ClusterOptions) *RedisClientFactory {
	if oneRedisClientFactory == nil {
		oneRedisClientFactory = &RedisClientFactory{
			store:   sync.Map{},
			options: op,
		}
	}
	return oneRedisClientFactory
}

func (this *RedisClientFactory) getClientKey(node *redisNode) string {
	return fmt.Sprintf("%s:%s", node.Ip, node.Port)
}

func (this *RedisClientFactory) CleanStore() {
	this.store.Range(func(k, v interface{}) bool {
		this.store.Delete(k)
		return true
	})
}

func (this *RedisClientFactory) getCurOptions(node *redisNode) *goredis.Options {
	return &goredis.Options{
		Addr:         fmt.Sprintf("%s:%s", node.Ip, node.Port),
		Password:     this.options.Password,
		DB:           0,
		PoolSize:     this.options.PoolSize,
		PoolTimeout:  this.options.PoolTimeout,
		DialTimeout:  this.options.DialTimeout,
		ReadTimeout:  this.options.ReadTimeout,
		WriteTimeout: this.options.WriteTimeout,
		IdleTimeout:  this.options.IdleTimeout,
		MaxRetries:   this.options.MaxRetries,
	}
}

func (this *RedisClientFactory) GetRedisClient(nodeGroup *redisGroup, isWrite bool) (*RedisClient, error) {
	// to select the slot node
	var hitNode *redisNode

	if isWrite {
		hitNode = nodeGroup.master
	} else {
		allNodes := []*redisNode{}
		allNodes = append(allNodes, nodeGroup.slaves...)
		if !this.options.ReadOnly {
			allNodes = append(allNodes, nodeGroup.master)
		}
		nodesLen := len(allNodes)
		if nodesLen == 0 {
		} else if nodesLen == 1 {
			hitNode = allNodes[0]
		} else {
			// to get one node by rand.
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			hitNode = allNodes[r.Intn(nodesLen)]
		}
	}

	if hitNode == nil {
		return nil, errors.New("redis nodes were empty.")
	}

	// to load the redis client by key.
	hitKey := this.getClientKey(hitNode)
	if rcInf, isExists := this.store.Load(hitKey); isExists {
		return rcInf.(*RedisClient), nil
	}

	// new a group client object.
	var newClient *RedisClient
	var err error
	if newClient, err = NewRedisClient(this.getCurOptions(hitNode)); err != nil {
		return newClient, err
	}

	// cache the new client object.
	this.store.Store(hitKey, newClient)

	return newClient, nil
}
