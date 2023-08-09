// Copyright (C) 2022
// Author FrankXu <frankxury@gmail.com>
// Build on 2022/07/11

// The redis test.

package redis

import (
	//"crypto/tls"
	"context"
	"fmt"
	goredis "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"strconv"
	"sync"
	"testing"
	"time"
)

var (
	testctx = context.Background()
)

func newRedis() (*RedisCluster, error) {
	rdb, err := NewClusterClient(
		testctx,
		&goredis.ClusterOptions{
			Addrs:    []string{"172.17.0.1:8001", "172.17.0.1:8002", "172.17.0.1:8003", "172.17.0.1:8004", "172.17.0.1:8005", "172.17.0.1:8006"},
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

	rdb.Set(testctx, "test-0", "value-0", 100*time.Second)
	rdb.Set(testctx, "test-1", "value-1", 100*time.Second)
	rdb.Set(testctx, "test-2", "value-2", 100*time.Second)

	AllMaxRun := MAX_COUCUR
	wg := sync.WaitGroup{}
	wg.Add(AllMaxRun)

	for i := 0; i < AllMaxRun; i++ {
		go func(wg *sync.WaitGroup, idx int) {
			defer wg.Done()

			for i := 0; i < 500; i++ {
				key := "test-" + strconv.Itoa(i%3)
				val, err := rdb.Get(testctx, key).Result()
				if err == goredis.Nil {
					fmt.Println("job-" + strconv.Itoa(idx) + " " + key + " does not exist")
				} else if err != nil {
					fmt.Printf("err : %s\n", err.Error())
				} else {
					fmt.Printf("%s Job-%d %s = %s-%d \n", time.Now().Format("2006-01-02 15:04:05"), idx, key, val, i)
				}
				//time.Sleep(500 * time.Millisecond)
			}
		}(&wg, i)
	}

	wg.Wait()

	stats := rdb.PoolStats()
	fmt.Printf("Hits=%d Misses=%d Timeouts=%d TotalConns=%d IdleConns=%d StaleConns=%d\n",
		stats.Hits, stats.Misses, stats.Timeouts, stats.TotalConns, stats.IdleConns, stats.StaleConns)
}

func TestSetAndGet(t *testing.T) {
	rdb, err := newRedis()
	if err != nil {
		fmt.Printf("new error: %s\n", err.Error())
		return
	}

	testCases := [10][2]string{}
	for i := 0; i < 10; i++ {
		testCases[i] = [2]string{fmt.Sprintf("t%d", i), fmt.Sprintf("abc%d", i)}
		rdb.Set(testctx, testCases[i][0], testCases[i][1], 3600*time.Second)
	}

	assert := assert.New(t)
	mutlKey := []string{}
	for i, one := range testCases {
		res, err := rdb.Get(testctx, one[0]).Result()
		fmt.Printf("Get res: %#v, %#v\n", res, err)
		assert.Equal(res, one[1], "test failed.")

		mutlKey = append(mutlKey, fmt.Sprintf("t%d", i+5))
	}

	res, err := rdb.Exists(testctx, mutlKey...).Result()
	fmt.Printf("Is exists: %#v, %#v\n", res, err)
	assert.Equal(res, int64(5), "test failed.")

	res, err = rdb.Exists(testctx, []string{testCases[0][0]}...).Result()
	fmt.Printf("Is exists: %#v, %#v\n", res, err)
	assert.Equal(res, int64(1), "test failed.")
}

func TestMSetAndMGet(t *testing.T) {
	rdb, err := newRedis()
	if err != nil {
		fmt.Printf("new error: %s\n", err.Error())
		return
	}

	assert := assert.New(t)
	testCases := []interface{}{}
	keys := []string{}
	for i := 0; i < 10; i++ {
		curKey := fmt.Sprintf("t%d", i)
		keys = append(keys, curKey)
		testCases = append(testCases, curKey)
		testCases = append(testCases, fmt.Sprintf("val-%d", i))
	}

	res := rdb.MSet(testctx, 300*time.Second, testCases...)
	fmt.Printf("MSet Res: %#v\n", res)

	getRes := rdb.MGet(testctx, keys...)
	fmt.Printf("MGet Res: %#v\n", getRes)

	type data struct {
		T0  string `redis:"t0"`
		T1  string `redis:"t1"`
		T2  string `redis:"t2"`
		T3  string `redis:"t3"`
		T4  string `redis:"t4"`
		T5  string `redis:"t5"`
		T6  string `redis:"t6"`
		T7  string `redis:"t7"`
		T8  string `redis:"t8"`
		T9  string `redis:"t9"`
		T10 string `redis:"t10"`
	}
	var d data
	err = getRes.Scan(&d)

	assert.Equal(err, nil, "test err failed.")
	assert.Equal(d, data{"val-0", "val-1", "val-2", "val-3", "val-4", "val-5", "val-6", "val-7", "val-8", "val-9", ""}, "test data failed.")
}

func TestDel(t *testing.T) {
	rdb, err := newRedis()
	if err != nil {
		fmt.Printf("new error: %s\n", err.Error())
		return
	}

	keys := []string{}
	for i := 0; i < 100; i++ {
		curKey := fmt.Sprintf("test-%d", i)
		keys = append(keys, curKey)
		rdb.Set(testctx, curKey, fmt.Sprintf("value-%d", i), 100*time.Second)
	}

	res := rdb.Exists(testctx, keys...)
	fmt.Printf("RES: %#v\n", res)

	assert := assert.New(t)
	assert.Equal(res.Val() == 100, true, "test save failed")

	res = rdb.Del(testctx, keys...)
	fmt.Printf("Del Res: %#v\n", res)
	assert.Equal(res.Val() == 100, true, "test delete failed")

	res = rdb.Exists(testctx, keys...)
	assert.Equal(res.Val() == 0, true, "test delete failed")

	res = rdb.Del(testctx, keys...)
	fmt.Printf("Del2 Res: %#v\n", res)
}

func TestPipeline(t *testing.T) {
	rdb, err := newRedis()
	if err != nil {
		fmt.Printf("new error: %s\n", err.Error())
		return
	}

	pipe := rdb.Pipeline()

	for i := 0; i < 10; i++ {
		pipe.Set(testctx, fmt.Sprintf("test-%d", i), fmt.Sprintf("val-%d", i), 300*time.Second)
	}

	for i := 0; i < 10; i++ {
		pipe.Expire(testctx, fmt.Sprintf("test-%d", i), 3600*time.Second)
	}

	for i := 0; i < 50; i++ {
		pipe.LPush(testctx, "test-list", i+1)
	}

	pipe.Exec(testctx)

	assert := assert.New(t)
	for i := 0; i < 10; i++ {
		res, _ := rdb.Get(testctx, fmt.Sprintf("test-%d", i)).Result()
		assert.Equal(res, fmt.Sprintf("val-%d", i), fmt.Sprintf("test %d failed", i))
	}
}

func BenchmarkMSetAndMGet(b *testing.B) {
	rdb, err := newRedis()
	if err != nil {
		fmt.Printf("new error: %s\n", err.Error())
		return
	}

	keys := []string{}
	kvs := []interface{}{}

	wg := sync.WaitGroup{}
	for i := 0; i < 20000; i++ {
		curKey := fmt.Sprintf("test-%d", i)
		keys = append(keys, curKey)
		kvs = append(kvs, curKey)
		kvs = append(kvs, fmt.Sprintf("val-%d", i))

		if i > 0 && i%100 == 0 {
			go func(wg *sync.WaitGroup, kvs []interface{}) {
				wg.Add(1)
				res := rdb.MSet(testctx, 300*time.Second, kvs...)
				err := res.Err()
				if err != nil {
					fmt.Printf("Set status: %#v\n", err)
				}
				wg.Done()
			}(&wg, kvs)

			go func(wg *sync.WaitGroup, keys []string) {
				wg.Add(1)
				res := rdb.MGet(testctx, keys...)
				err := res.Err()
				if err != nil {
					fmt.Printf("Get status: %#v\n", err)
				}
				wg.Done()
			}(&wg, keys)

			keys = []string{}
			kvs = []interface{}{}
		}
	}

	wg.Wait()

	stats := rdb.PoolStats()
	fmt.Printf("Hits=%d Misses=%d Timeouts=%d TotalConns=%d IdleConns=%d StaleConns=%d\n",
		stats.Hits, stats.Misses, stats.Timeouts, stats.TotalConns, stats.IdleConns, stats.StaleConns)
}
