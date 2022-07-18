// Copyright (C) 2022
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

const (
	MAX_COUCUR = 6

	VERSION = "0.0.3"
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

type ClusterOptions = goredis.ClusterOptions

func NewClusterClient(ctx context.Context, opt *ClusterOptions) (*RedisCluster, error) {
	core := goredis.NewClusterClient(opt)

	obj := &RedisCluster{core, nil, nil}
	if err := obj.initClustInfo(ctx); err != nil {
		return nil, err
	}

	return obj, nil
}

func (this *RedisCluster) initClustInfo(ctx context.Context) error {
	var clusterInfo *clusterInfo
	var nodes *redisNodes

	if ci, err := this.ClusterInfo(ctx).Result(); err != nil {
		return err
	} else if cn, err := this.ClusterNodes(ctx).Result(); err != nil {
		return err
	} else {
		clusterInfo = NewClusterInfo(ci)
		if clusterInfo.Cluster_state != "ok" {
			return errors.New("cluster is not OK")
		}

		if nodes, err = NewRedisNodes(cn); err != nil {
			return err
		}

		this.clusterInfo = clusterInfo
		this.nodes = nodes
	}

	return nil
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

			keyNodesMap[hitNodeGP.master.Id].Keys = append(keyNodesMap[hitNodeGP.master.Id].Keys, key)
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

// Refactor the Del method.
func (this *RedisCluster) Del(ctx context.Context, keys ...string) *goredis.IntCmd {
	keyInfs := append([]interface{}{"del"}, this.strArr2InfArr(keys)...)
	getError := func(err error) *goredis.IntCmd {
		iCmd := goredis.NewIntCmd(ctx, keyInfs...)
		iCmd.SetErr(err)
		return iCmd
	}

	triedTimes := 0

TryAgain:

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
			if NewRedisHelper().IsMovedError(err) {
				this.initClustInfo(this.ClusterClient.Context())
				if triedTimes < 3 {
					triedTimes += 1
					goto TryAgain
				}
			}

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
	if len(keys) == 1 {
		return this.ClusterClient.Exists(ctx, keys...)
	}

	triedTimes := 0

TryAgain:

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

			curPipe := curClient.Pipeline()
			resArr := []*goredis.IntCmd{}
			for _, curKey := range curNode.Keys {
				res := curPipe.Exists(ctx, curKey)
				resArr = append(resArr, res)
			}

			_, err = curPipe.Exec(ctx)
			if err != nil && err != goredis.Nil {
				curRes.Err = err
				resCh <- curRes
				return
			}

			var curTotal int64 = 0
			for _, one := range resArr {
				curTotal += one.Val()
			}

			curRes.Val = curTotal
			resCh <- curRes
		}(resCh, node)
	}

	result := goredis.NewIntCmd(ctx, keyInfs...)
	var totalVal int64 = 0

	for i := 0; i < mapLen; i++ {
		if curRes := <-resCh; curRes.Err != nil {
			if NewRedisHelper().IsMovedError(curRes.Err) {
				this.initClustInfo(this.ClusterClient.Context())
				if triedTimes < 3 {
					triedTimes += 1
					goto TryAgain
				}
			}

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
func (this *RedisCluster) MSet(ctx context.Context, dur time.Duration, values ...interface{}) *goredis.StatusCmd {
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

	triedTimes := 0

TryAgain:

	keyNodesMap := this.getKeyNodesMap(keys)
	mapLen := len(keyNodesMap)

	type curResultModel struct {
		Err error
		Val string
	}
	var resCh chan *curResultModel = make(chan *curResultModel, mapLen)

	redisFactory := NewRedisClientFactory(this.Options())

	// MSet by group Pipeline.
	for _, node := range keyNodesMap {
		go func(resCh chan *curResultModel, curNode *hitKeysItem) {
			curRes := &curResultModel{}
			curClient, err := redisFactory.GetRedisClient(curNode.HitNodeGP, true)
			if err != nil {
				curRes.Err = err
				resCh <- curRes
				return
			}

			curPipe := curClient.Pipeline()
			resArr := []*goredis.StatusCmd{}
			for _, curKey := range curNode.Keys {
				resArr = append(resArr, curPipe.Set(ctx, curKey, keyValMap[curKey], dur))
			}

			_, err = curPipe.Exec(ctx)
			if err != nil {
				curRes.Err = err
				resCh <- curRes
				return
			}

			for _, one := range resArr {
				if one.Val() != "OK" {
					curRes.Val = "NO"
					curRes.Err = one.Err()
					resCh <- curRes
					return
				}
			}

			curRes.Val = "OK"
			resCh <- curRes
		}(resCh, node)
	}

	// merge the results.
	result := goredis.NewStatusCmd(ctx, cmdKeys)
	var lastStatus = ""

	for i := 0; i < mapLen; i++ {
		if curRes := <-resCh; curRes.Err != nil {
			if NewRedisHelper().IsMovedError(curRes.Err) {
				this.initClustInfo(this.ClusterClient.Context())
				if triedTimes < 3 {
					triedTimes += 1
					goto TryAgain
				}
			}

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

// Refactor the MGet method.
func (this *RedisCluster) MGet(ctx context.Context, keys ...string) *goredis.SliceCmd {
	cmdKeys := append([]interface{}{"mget"}, this.strArr2InfArr(keys)...)

	triedTimes := 0

TryAgain:

	// init params.
	keyNodesMap := this.getKeyNodesMap(keys)
	mapLen := len(keyNodesMap)

	type curResultModel struct {
		Err error
		Val map[string]*goredis.StringCmd
	}
	var resCh chan *curResultModel = make(chan *curResultModel, mapLen)

	redisFactory := NewRedisClientFactory(this.Options())

	// MGet by group.
	for _, node := range keyNodesMap {
		go func(resCh chan *curResultModel, curNode *hitKeysItem) {
			curRes := &curResultModel{}
			curClient, err := redisFactory.GetRedisClient(curNode.HitNodeGP, true)
			if err != nil {
				curRes.Err = err
				resCh <- curRes
				return
			}

			curPipe := curClient.Pipeline()
			resMap := map[string]*goredis.StringCmd{}
			for _, curKey := range curNode.Keys {
				resMap[curKey] = curPipe.Get(ctx, curKey)
			}

			_, err = curPipe.Exec(ctx)
			if err != nil && err != goredis.Nil {
				curRes.Err = err
				resCh <- curRes
				return
			}

			curRes.Val = resMap
			resCh <- curRes
		}(resCh, node)
	}

	// merge the results.
	var vals = []interface{}{}

	sCms := goredis.NewSliceCmd(ctx, cmdKeys...)
	resultMap := map[string]*goredis.StringCmd{}
	for i := 0; i < mapLen; i++ {
		if curRes := <-resCh; curRes.Err != nil {
			if NewRedisHelper().IsMovedError(curRes.Err) {
				this.initClustInfo(this.ClusterClient.Context())
				if triedTimes < 3 {
					triedTimes += 1
					goto TryAgain
				}
			}

			sCms.SetErr(curRes.Err)
			return sCms
		} else {
			for key, curCmd := range curRes.Val {
				resultMap[key] = curCmd
			}
		}
	}

	for _, curKey := range keys {
		vals = append(vals, resultMap[curKey].Val())
	}

	sCms.SetVal(vals)

	return sCms
}
