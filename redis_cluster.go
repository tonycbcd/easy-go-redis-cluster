// Copyright 2022, SuccessfulMatch.com All rights reserved.
// Author FrankXu <frankxury@gmail.com>
// Build on 2022/07/01

// The redis cluster features.

package redis

import (
	"log"
	"time"
	//"sync"
	"context"
	"errors"
	goredis "github.com/go-redis/redis/v8"
)

var (
	ctx = context.Background()
)

const (
	MAX_COUCUR = 6

	VERSION = "0.0.1"
)

type RedisCluster struct {
	*goredis.ClusterClient

	clusterInfo *clusterInfo
	nodes       *redisNodes
}

type hitKeysItem struct {
	Keys      []string
	HitNodeGP *redisGroup
}

func NewClusterClient(opt *goredis.ClusterOptions) (*RedisCluster, error) {
	core := goredis.NewClusterClient(opt)

	var clusterInfo *clusterInfo
	var nodes *redisNodes

	if ci, err := core.ClusterInfo(ctx).Result(); err != nil {
		return nil, err
	} else if cn, err := core.ClusterNodes(ctx).Result(); err != nil {
		return nil, err
	} else {
		clusterInfo = NewClusterInfo(ci)
		if clusterInfo.Cluster_state != "ok" {
			return nil, errors.New("cluster is not OK")
		}

		if nodes, err = NewRedisNodes(cn); err != nil {
			return nil, err
		}
	}

	return &RedisCluster{core, clusterInfo, nodes}, nil
}

func (this *RedisCluster) getKeyNodesMap(keys []string) map[string]*hitKeysItem {
	keyNodesMap := map[string]*hitKeysItem{}
	crc16Handle := NewCRC16()

	for _, key := range keys {
		curCRC16Val := crc16Handle.HashSlot(key)
		if hitNodeGP, isFound := this.nodes.FindNodeByCRC16Val(curCRC16Val); isFound {
			if _, isExists := keyNodesMap[hitNodeGP.master.Id]; !isExists {
				keyNodesMap[hitNodeGP.master.Id] = &hitKeysItem{
					Keys:      []string{},
					HitNodeGP: hitNodeGP,
				}
			}

			keyNodesMap[hitNodeGP.master.Id].Keys = append(keyNodesMap[hitNodeGP.master.Id].Keys, hitNodeGP.master.RebuildKey(key))
		} else {
			log.Printf("The slot node has not been found for the key '%s'", key)
		}
	}

	return keyNodesMap
}

func (this *RedisCluster) getHitGroupInMap(key string, hitMap map[string]*hitKeysItem) (*redisGroup, error) {
	if hitMap == nil {
		return nil, errors.New("hitMap was empty.")
	}

	var hitGroup *redisGroup
	for curKey, curItem := range hitMap {
		if key == "" || key == curKey {
			hitGroup = curItem.HitNodeGP
			break
		}
	}

	if hitGroup == nil {
		return nil, errors.New("no any hitted group.")
	}

	return hitGroup, nil
}

func (this *RedisCluster) strArr2InfArr(keys []string) []interface{} {
	infArr := []interface{}{}
	for _, one := range keys {
		infArr = append(infArr, one)
	}
	return infArr
}

// Refactor the Set method.
func (this *RedisCluster) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *goredis.StatusCmd {
	getError := func(err error) *goredis.StatusCmd {
		sCms := goredis.NewStatusCmd(ctx, key, value, expiration)
		sCms.SetErr(err)
		return sCms
	}

	hitGroup, err := this.getHitGroupInMap("", this.getKeyNodesMap([]string{key}))
	if err != nil {
		return getError(err)
	}

	redisFactory := NewRedisClientFactory(this.Options())
	curClient, err := redisFactory.GetRedisClient(hitGroup, true)
	if err != nil {
		return getError(err)
	}

	return curClient.Set(ctx, hitGroup.master.RebuildKey(key), value, expiration)
}

// Refactor the Get method.
func (this *RedisCluster) Get(ctx context.Context, key string) *goredis.StringCmd {
	getError := func(err error) *goredis.StringCmd {
		sCms := goredis.NewStringCmd(ctx, key)
		sCms.SetErr(err)
		return sCms
	}

	hitGroup, err := this.getHitGroupInMap("", this.getKeyNodesMap([]string{key}))
	if err != nil {
		return getError(err)
	}

	redisFactory := NewRedisClientFactory(this.Options())
	curClient, err := redisFactory.GetRedisClient(hitGroup, true)
	if err != nil {
		return getError(err)
	}

	return curClient.Get(ctx, hitGroup.master.RebuildKey(key))
}

// Refactor the Del method.
func (this *RedisCluster) Del(ctx context.Context, keys ...string) *goredis.IntCmd {
	// TODO
	return this.ClusterClient.Del(ctx, keys...)
}

func (this *RedisCluster) Exists(ctx context.Context, keys ...string) *goredis.IntCmd {
	keyNodesMap := this.getKeyNodesMap(keys)

	redisFactory := NewRedisClientFactory(this.Options())

	keyInfs := this.strArr2InfArr(keys)
	result := goredis.NewIntCmd(ctx, keyInfs...)

	var totalVal int64 = 0
	for _, node := range keyNodesMap {
		curClient, _ := redisFactory.GetRedisClient(node.HitNodeGP, true)
		subRes := curClient.Exists(ctx, node.Keys...)
		if curVal, err := subRes.Result(); err != nil {
			result.SetErr(err)
			return result
		} else {
			totalVal += curVal
		}
	}
	result.SetVal(totalVal)

	return result
}

func (this *RedisCluster) MGet(ctx context.Context, keys ...string) *goredis.SliceCmd {
	// TODO
	return this.ClusterClient.MGet(ctx, keys...)
}

func (this *RedisCluster) MSet(ctx context.Context, values ...interface{}) *goredis.StatusCmd {
	// TODO
	return this.ClusterClient.MSet(ctx, values...)
}

func (this *RedisCluster) MSetNX(ctx context.Context, values ...interface{}) *goredis.BoolCmd {
	// TODO
	return this.ClusterClient.MSetNX(ctx, values...)
}

func (this *RedisCluster) Pipeline() goredis.Pipeliner {
	// TODO
	return this.ClusterClient.Pipeline()
}

/*func (this *RedisCluster) Pipelined(ctx context.Context, fn func(goredis.Pipeliner) error) ([]goredis.Cmder, error)
    // TODO
    return this.ClusterClient.Pipelined(ctx, fn)
}*/

func (this *RedisCluster) TxPipeline() goredis.Pipeliner {
	// TODO
	return this.ClusterClient.TxPipeline()
}

func (this *RedisCluster) TxPipelined(ctx context.Context, fn func(goredis.Pipeliner) error) ([]goredis.Cmder, error) {
	// TODO
	return this.ClusterClient.TxPipelined(ctx, fn)
}
