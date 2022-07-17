// Copyright (C) 2022
// Author FrankXu <frankxury@gmail.com>
// Build on 2022/07/15

// The redis cluster helper.

package redis

import (
	"errors"
	"regexp"
)

type RedisHelper struct{}

var oneRedisHelper *RedisHelper

func NewRedisHelper() *RedisHelper {
	if oneRedisHelper == nil {
		oneRedisHelper = &RedisHelper{}
	}
	return oneRedisHelper
}

func (this *RedisHelper) GetKeysInPairInfs(vals []interface{}) ([]string, map[string]interface{}, error) {
	keys := []string{}
	keyValMap := map[string]interface{}{}
	curKey := ""
	var isOk bool

	for i, one := range vals {
		if i%2 == 0 {
			if curKey, isOk = one.(string); isOk {
				keys = append(keys, curKey)
			} else {
				return keys, keyValMap, errors.New("The key was not a string.")
			}
		} else {
			keyValMap[curKey] = one
		}
	}

	return keys, keyValMap, nil
}

func (this *RedisHelper) RemoveRedisHashTag(key string) string {
	re := regexp.MustCompile(`^{.+?}:`)
	return string(re.ReplaceAll([]byte(key), []byte("")))
}

func (this *RedisHelper) RemoveRedisHashTags(keys []string) []string {
	newKeys := []string{}
	for _, one := range keys {
		newKeys = append(newKeys, this.RemoveRedisHashTag(one))
	}
	return newKeys
}

func (this *RedisHelper) RestorePartInfs(keys []string, keyValMap map[string]interface{}) []interface{} {
	infs := []interface{}{}

	for _, one := range keys {
		if val, isExists := keyValMap[this.RemoveRedisHashTag(one)]; isExists {
			infs = append(infs, one)
			infs = append(infs, val)
		}
	}

	return infs
}
