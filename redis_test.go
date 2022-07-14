// Copyright 2022, SuccessfulMatch.com All rights reserved.
// Author FrankXu <frankxury@gmail.com>
// Build on 2022/07/11

// The redis test.

package redis

import (
	//"crypto/tls"
	"fmt"
	goredis "github.com/go-redis/redis/v8"
	"strconv"
	"sync"
	"testing"
	"time"
)

func newRedis() (*RedisCluster, error) {
	rdb, err := NewClusterClient(&goredis.ClusterOptions{
		Addrs:    []string{"172.17.0.1:8001"},
		Password: "",
		//连接池容量及闲置连接数量
		PoolSize:     10, // 连接池最大socket连接数，默认为4倍CPU数， 4 * runtime.NumCPU
		MinIdleConns: 10, //在启动阶段创建指定数量的Idle连接，并长期维持idle状态的连接数不少于指定数量；。

		//超时
		DialTimeout:  5 * time.Second, //连接建立超时时间，默认5秒。
		ReadTimeout:  3 * time.Second, //读超时，默认3秒， -1表示取消读超时
		WriteTimeout: 3 * time.Second, //写超时，默认等于读超时
		PoolTimeout:  4 * time.Second, //当所有连接都处在繁忙状态时，客户端等待可用连接的最大等待时长，默认为读超时+1秒。

		//闲置连接检查包括IdleTimeout，MaxConnAge
		IdleCheckFrequency: 60 * time.Second, //闲置连接检查的周期，默认为1分钟，-1表示不做周期性检查，只在客户端获取连接时对闲置连接进行处理。
		IdleTimeout:        5 * time.Minute,  //闲置超时，默认5分钟，-1表示取消闲置超时检查
		MaxConnAge:         0 * time.Second,  //连接存活时长，从创建开始计时，超过指定时长则关闭连接，默认为0，即不关闭存活时长较长的连接

		//命令执行失败时的重试策略
		MaxRetries:      10,                     // 命令执行失败时，最多重试多少次，默认为0即不重试
		MinRetryBackoff: 8 * time.Millisecond,   //每次计算重试间隔时间的下限，默认8毫秒，-1表示取消间隔
		MaxRetryBackoff: 512 * time.Millisecond, //每次计算重试间隔时间的上限，默认512毫秒，-1表示取消间隔

		/*TLSConfig: &tls.Config{
			InsecureSkipVerify: true,
		},*/

		// ReadOnly = true，只择 Slave Node
		// ReadOnly = true 且 RouteByLatency = true 将从 slot 对应的 Master Node 和 Slave Node， 择策略为: 选择PING延迟最低的点
		// ReadOnly = true 且 RouteRandomly = true 将从 slot 对应的 Master Node 和 Slave Node 选择，选择策略为: 随机选择

		ReadOnly:       false,
		RouteRandomly:  true,
		RouteByLatency: true,
	})

	return rdb, err
}

func TestRedisCluster(t *testing.T) {
	rdb, _ := newRedis()
	defer rdb.Close()

	rdb.Set(ctx, "test-0", "value-0", 100*time.Second)
	rdb.Set(ctx, "test-1", "value-1", 100*time.Second)
	rdb.Set(ctx, "test-2", "value-2", 100*time.Second)

	AllMaxRun := MAX_COUCUR
	wg := sync.WaitGroup{}
	wg.Add(AllMaxRun)

	for i := 0; i < AllMaxRun; i++ {
		go func(wg *sync.WaitGroup, idx int) {
			defer wg.Done()

			for i := 0; i < 500; i++ {
				key := "test-" + strconv.Itoa(i%3)
				val, err := rdb.Get(ctx, key).Result()
				if err == goredis.Nil {
					fmt.Println("job-" + strconv.Itoa(idx) + " " + key + " does not exist")
				} else if err != nil {
					fmt.Printf("err : %s\n", err.Error())
				} else {
					fmt.Printf("%s Job-%d %s = %s-%d \n", time.Now().Format("2006-01-02 15:04:05"), idx, key, val, i)
				}
				time.Sleep(500 * time.Millisecond)
			}
		}(&wg, i)
	}

	wg.Wait()

	stats := rdb.PoolStats()
	fmt.Printf("Hits=%d Misses=%d Timeouts=%d TotalConns=%d IdleConns=%d StaleConns=%d\n",
		stats.Hits, stats.Misses, stats.Timeouts, stats.TotalConns, stats.IdleConns, stats.StaleConns)
}

func TestParseRedis(t *testing.T) {
	rdb, err := newRedis()
	if err != nil {
		fmt.Printf("new error: %s\n", err.Error())
		return
	}

	fmt.Printf("cluster master: %#v\n", rdb.nodes.groupMap["155738c392dfbb522ab1472c719a57f66ab5bf20"].master)

	for i := 0; i < 10; i++ {
		rdb.Set(ctx, fmt.Sprintf("t%d", i), fmt.Sprintf("abc%d", i), 3600*time.Second)
	}

	mutlKey := []string{}
	for i := 0; i < 10; i++ {
		res, err := rdb.Get(ctx, fmt.Sprintf("t%d", i)).Result()
		fmt.Printf("Get res: %#v, %#v\n", res, err)

		oKey := fmt.Sprintf("t%d", i+5)
		mutlKey = append(mutlKey, oKey)
		res1, err := rdb.Exists(ctx, oKey).Result()
		fmt.Printf("Is %s exists: %#v, %#v\n", oKey, res1, err)
	}

	res, err := rdb.Exists(ctx, mutlKey...).Result()
	fmt.Printf("Is exists: %#v, %#v\n", res, err)

	//curName, err := rdb.ClusterNodes(ctx).Result()
	//fmt.Printf("Current client name: %#v, %#v\n", curName, err)
}