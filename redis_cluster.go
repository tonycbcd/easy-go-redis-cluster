// Copyright 2022, SuccessfulMatch.com All rights reserved.
// Author FrankXu <frankxury@gmail.com>
// Build on 2022/07/01

// The redis cluster features.

package redis

import (
    "fmt"
    "log"
    //"sync"
    "errors"
    "context"
	goredis "github.com/go-redis/redis/v8"
)

var (
    ctx = context.Background()
)

const (
    MAX_COUCUR      = 6
)

type redisCluster struct {
	*goredis.ClusterClient

    clusterInfo     *clusterInfo
    nodes           *redisNodes
}

type hitKeysItem struct {
    Keys            []string
    HitNodeGP       *redisGroup
}

func NewClusterClient(opt *goredis.ClusterOptions) (*redisCluster, error) {
	core := goredis.NewClusterClient(opt)

    var clusterInfo *clusterInfo
    var nodes *redisNodes

    if ci, err := core.ClusterInfo(ctx).Result(); err != nil {
        return nil, err
    } else if cn, err := core.ClusterNodes(ctx).Result(); err != nil {
        return nil, err
    } else {
        clusterInfo    = NewClusterInfo(ci)
        if clusterInfo.Cluster_state != "ok" {
            return nil, errors.New("cluster is not OK")
        }

        if nodes, err = NewRedisNodes(cn); err != nil {
            return nil, err
        }
    }

	return &redisCluster{core, clusterInfo, nodes}, nil
}

func (this *redisCluster) getKeyNodesMap(keys []string) map[string]*hitKeysItem {
    keyNodesMap := map[string]*hitKeysItem{}
    crc16Handle := NewCRC16()

    for _, key := range keys {
        curCRC16Val := crc16Handle.Encode(key)
        if hitNodeGP, isFound := this.nodes.FindNodeByCRC16Val(curCRC16Val); isFound {
            if _, isExists := keyNodesMap[ hitNodeGP.master.Id ]; !isExists {
                keyNodesMap[ hitNodeGP.master.Id ]  = &hitKeysItem{
                    Keys:       []string{},
                    HitNodeGP:  hitNodeGP,
                }
            }

            keyNodesMap[ hitNodeGP.master.Id ].Keys = append(keyNodesMap[ hitNodeGP.master.Id ].Keys, key)
        } else {
            log.Printf("The key '%s' has not been found the slot node", key)
        }
    }

    return keyNodesMap
}

func (this *redisCluster) Del(ctx context.Context, keys ...string) *goredis.IntCmd {
    // TODO
    return this.ClusterClient.Del(ctx, keys...)
}

func (this *redisCluster) Exists(ctx context.Context, keys ...string) *goredis.IntCmd {
    if len(keys) == 1 {
        return this.ClusterClient.Exists(ctx, keys...)
    }
    keyNodesMap := this.getKeyNodesMap(keys)
fmt.Printf("TEST keyNodesMap: %#v\n", keyNodesMap)

    return this.ClusterClient.Exists(ctx, keys...)
}


func (this *redisCluster) MGet(ctx context.Context, keys ...string) *goredis.SliceCmd {
    // TODO
    return this.ClusterClient.MGet(ctx, keys...)
}

func (this *redisCluster) MSet(ctx context.Context, values ...interface{}) *goredis.StatusCmd {
    // TODO
    return this.ClusterClient.MSet(ctx, values...)
}

func (this *redisCluster) MSetNX(ctx context.Context, values ...interface{}) *goredis.BoolCmd {
    // TODO
    return this.ClusterClient.MSetNX(ctx, values...)
}

func (this *redisCluster) Pipeline() goredis.Pipeliner {
    // TODO
    return this.ClusterClient.Pipeline()
}

/*func (this *redisCluster) Pipelined(ctx context.Context, fn func(goredis.Pipeliner) error) ([]goredis.Cmder, error)
    // TODO
    return this.ClusterClient.Pipelined(ctx, fn)
}*/

func (this *redisCluster) TxPipeline() goredis.Pipeliner {
    // TODO
    return this.ClusterClient.TxPipeline()
}

func (this *redisCluster) TxPipelined(ctx context.Context, fn func(goredis.Pipeliner) error) ([]goredis.Cmder, error) {
    // TODO
    return this.ClusterClient.TxPipelined(ctx, fn)
}

