package redis

import (
	"context"
	"os"
	"strconv"
	"strings"
	"time"

	r "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

var (
	rdb *r.ClusterClient // Redis
)

/*
	New redis command
*/
func InitRedis(logger *zap.SugaredLogger) {
	logger.Info("init redis")
	var hosts = ":6379"
	if v := os.Getenv("REDIS_SERVER"); len(v) > 0 {
		hosts = v
	} else {
		logger.Error("REDIS_SERVER envirment not found")
	}
	var password = ""
	if v := os.Getenv("REDIS_PASSWORD"); len(v) > 0 {
		password = v
	} else {
		logger.Error("REDIS_PASSWORD envirment not found")
	}

	rdb = r.NewClusterClient(&r.ClusterOptions{
		Addrs:    strings.Split(hosts, ","),
		Password: password,
	})
}

func GetRedis() *r.ClusterClient {
	return rdb
}

/*
 	Set redis value
		Paramters
			key: redis key
*/
func SetValue(key string, value interface{}, expire int) error {
	ctx := context.Background()
	if expire <= 0 {
		expire = 0
	}
	_, err := rdb.Set(ctx, key, value, time.Second*time.Duration(expire)).Result()
	return err
}

/*
	Get redis byte array format data
		Paramters
			key: redis key
*/
func GetBytes(key string) ([]byte, error) {
	ctx := context.Background()
	data, err := rdb.Get(ctx, key).Result()
	return []byte(data), err
}

/*
	Get redis string format data
		Paramters
			key: redis key
*/
func GetString(key string) (string, error) {
	ctx := context.Background()
	return rdb.Get(ctx, key).Result()
}

/*
	Get redis integer format data
		Paramters
			key: redis key
*/
func GetInt(key string) (int64, error) {
	ctx := context.Background()
	data, err := rdb.Get(ctx, key).Result()
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(data, 10, 64)
}

/*
	Get redis key TTL
		Paramters
			key: redis key
*/
func GetTTL(key string) (time.Duration, error) {
	ctx := context.Background()
	return rdb.TTL(ctx, key).Result()
}

/*
	Delete redis data
		Paramters
			key: redis key
*/
func DelKey(key string) error {
	ctx := context.Background()
	_, err := rdb.Del(ctx, key).Result()
	return err
}

/*
	Set redis array string data
		Paramters
			key: redis key
			data: array data
*/
func SetArrayStringData(key string, data []string, expire int) error {
	ctx := context.Background()
	_, err := rdb.Del(ctx, key).Result()
	if err != nil {
		return err
	}
	for _, d := range data {
		_, err := rdb.LPush(ctx, key, d).Result()
		if err != nil {
			return err
		}
	}
	if expire >= 0 {
		_, err = rdb.Expire(ctx, key, time.Second*time.Duration(expire)).Result()
	}
	return err
}

/*
	Get redis array string data
*/
func GetArrayStringData(key string) ([]string, error) {
	ctx := context.Background()
	return rdb.LRange(ctx, key, 0, -1).Result()
}
