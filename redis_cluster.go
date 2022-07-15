// Copyright 2022, SuccessfulMatch.com All rights reserved.
// Author FrankXu <frankxury@gmail.com>
// Build on 2022/07/01

// The redis cluster features.

package redis

import (
	"context"
	"errors"
	goredis "github.com/go-redis/redis/v8"
	"log"
	"time"
)

var (
	ctx = context.Background()
)

const (
	MAX_COUCUR = 6

	VERSION = "0.0.2"
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
	keyInfs := append([]interface{}{"del"}, this.strArr2InfArr(keys)...)
	getError := func(err error) *goredis.IntCmd {
		iCmd := goredis.NewIntCmd(ctx, keyInfs...)
		iCmd.SetErr(err)
		return iCmd
	}

	// init the params.
	keyNodesMap := this.getKeyNodesMap(keys)
	mapLen := len(keyNodesMap)
	redisFactory := NewRedisClientFactory(this.Options())

	var resCh chan *goredis.IntCmd = make(chan *goredis.IntCmd, mapLen)

	// To del by group.
	for _, node := range keyNodesMap {
		go func(resCh chan *goredis.IntCmd, curNode *hitKeysItem) {
			curClient, err := redisFactory.GetRedisClient(curNode.HitNodeGP, true)
			if err != nil {
				resCh <- getError(err)
				return
			}

			resCh <- curClient.Del(ctx, curNode.Keys...)
		}(resCh, node)
	}

	result := goredis.NewIntCmd(ctx, keyInfs...)
	var totalVal int64 = 0

	// merge the results.
	for i := 0; i < mapLen; i++ {
		curRes := <-resCh
		if curVal, err := curRes.Result(); err != nil {
			result.SetErr(err)
			return result
		} else {
			totalVal += curVal
		}
	}

	result.SetVal(totalVal)

	return result
}

func (this *RedisCluster) Exists(ctx context.Context, keys ...string) *goredis.IntCmd {
	keyNodesMap := this.getKeyNodesMap(keys)

	redisFactory := NewRedisClientFactory(this.Options())

	keyInfs := append([]interface{}{"exists"}, this.strArr2InfArr(keys)...)
	mapLen := len(keyNodesMap)

	type curResultModel struct {
		Err error
		Val int64
	}
	var resCh chan *curResultModel = make(chan *curResultModel, mapLen)

	for _, node := range keyNodesMap {
		go func(resCh chan *curResultModel, curNode *hitKeysItem) {
			curRes := &curResultModel{}
			curClient, err := redisFactory.GetRedisClient(curNode.HitNodeGP, true)
			if err != nil {
				curRes.Err = err
				resCh <- curRes
				return
			}

			subRes := curClient.Exists(ctx, curNode.Keys...)
			curVal, err := subRes.Result()
			curRes.Err = err
			curRes.Val = curVal
			resCh <- curRes
		}(resCh, node)
	}

	result := goredis.NewIntCmd(ctx, keyInfs...)
	var totalVal int64 = 0

	for i := 0; i < mapLen; i++ {
		if curRes := <-resCh; curRes.Err != nil {
			result.SetErr(curRes.Err)
			return result
		} else {
			totalVal += curRes.Val
		}
	}

	result.SetVal(totalVal)

	return result
}

// Refactor the MSet method.
func (this *RedisCluster) MSet(ctx context.Context, values ...interface{}) *goredis.StatusCmd {
	cmdKeys := append([]interface{}{"mset"}, values...)
	getError := func(err error) *goredis.StatusCmd {
		sCms := goredis.NewStatusCmd(ctx, cmdKeys...)
		sCms.SetErr(err)
		return sCms
	}

	// init params.
	var keys = []string{}
	var keyValMap = map[string]interface{}{}
	var err error
	var helper = NewRedisHelper()
	if keys, keyValMap, err = helper.GetKeysInPairInfs(values); err != nil {
		return getError(err)
	}

	keyNodesMap := this.getKeyNodesMap(keys)
	mapLen := len(keyNodesMap)

	type curResultModel struct {
		Err error
		Val string
	}
	var resCh chan *curResultModel = make(chan *curResultModel, mapLen)

	redisFactory := NewRedisClientFactory(this.Options())

	// MSet by group.
	for _, node := range keyNodesMap {
		go func(resCh chan *curResultModel, curNode *hitKeysItem) {
			curRes := &curResultModel{}
			curClient, err := redisFactory.GetRedisClient(curNode.HitNodeGP, true)
			if err != nil {
				curRes.Err = err
				resCh <- curRes
				return
			}

			curVals := helper.RestorePartInfs(curNode.Keys, keyValMap)
			subRes := curClient.MSet(ctx, curVals...)
			curVal, err := subRes.Result()
			curRes.Err = err
			curRes.Val = curVal
			resCh <- curRes
		}(resCh, node)
	}

	// merge the results.
	result := goredis.NewStatusCmd(ctx, cmdKeys)
	var lastStatus = ""

	for i := 0; i < mapLen; i++ {
		if curRes := <-resCh; curRes.Err != nil {
			result.SetErr(curRes.Err)
			return result
		} else {
			lastStatus = curRes.Val
			if curRes.Val != "OK" {
				break
			}
		}
	}

	result.SetVal(lastStatus)

	return result
}

// Refactor the MSetNX method.
func (this *RedisCluster) MSetNX(ctx context.Context, values ...interface{}) *goredis.BoolCmd {
	// TODO
	return this.ClusterClient.MSetNX(ctx, values...)
}

// Refactor the MGet method.
func (this *RedisCluster) MGet(ctx context.Context, keys ...string) *goredis.SliceCmd {
	cmdKeys := append([]interface{}{"mget"}, this.strArr2InfArr(keys)...)
	getError := func(err error) *goredis.SliceCmd {
		sCms := goredis.NewSliceCmd(ctx, cmdKeys...)
		sCms.SetErr(err)
		return sCms
	}

	// init params.
	keyNodesMap := this.getKeyNodesMap(keys)
	mapLen := len(keyNodesMap)

	type curResultModel struct {
		Keys []string
		SCmd *goredis.SliceCmd
	}
	var resCh chan *curResultModel = make(chan *curResultModel, mapLen)

	redisFactory := NewRedisClientFactory(this.Options())

	// MGet by group.
	for _, node := range keyNodesMap {
		go func(resCh chan *curResultModel, curNode *hitKeysItem) {
			curRes := &curResultModel{
				Keys: NewRedisHelper().RemoveRedisHashTags(curNode.Keys),
			}
			curClient, err := redisFactory.GetRedisClient(curNode.HitNodeGP, true)
			if err != nil {
				curRes.SCmd = getError(err)
				resCh <- curRes
				return
			}

			curRes.SCmd = curClient.MGet(ctx, curNode.Keys...)
			resCh <- curRes
		}(resCh, node)
	}

	// merge the results.
	var vals = []interface{}{}

	newKeys := []interface{}{"mget"}
	for i := 0; i < mapLen; i++ {
		curRes := <-resCh
		if resArr, err := curRes.SCmd.Result(); err != nil {
			return curRes.SCmd
		} else {
			newKeys = append(newKeys, this.strArr2InfArr(curRes.Keys)...)
			vals = append(vals, resArr...)
		}
	}

	sCms := goredis.NewSliceCmd(ctx, newKeys...)
	sCms.SetVal(vals)

	return sCms
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
