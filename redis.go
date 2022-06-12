package redis

import (
	"os"
	"runtime"
	"time"

	r "github.com/gomodule/redigo/redis"
	"go.uber.org/zap"
)

var (
	redisPool *r.Pool // Redis pool
	works     = runtime.NumCPU()
)

/*
	New redis command
*/
func InitRedis(logger *zap.SugaredLogger) {
	logger.Info("init redis")
	var host = "localhost"
	if v := os.Getenv("REDIS_HOST"); len(v) > 0 {
		host = v
	} else {
		logger.Error("REDIS_HOST envirment not found")
	}
	var port = "6379"
	if v := os.Getenv("REDIS_PORT"); len(v) > 0 {
		port = v
	} else {
		logger.Error("REDIS_PORT envirment not found")
	}

	redisServer := host + ":" + port
	logger.Infof("redis url is %s", redisServer)
	redisPassword := os.Getenv("REDIS_PASSWORD")
	redisPool = &r.Pool{
		MaxIdle:     works,
		IdleTimeout: 240 * time.Second,
		Dial: func() (r.Conn, error) {
			c, err := r.Dial("tcp", redisServer)
			if err != nil {
				logger.Fatalw("connect to redis error", "err", err)
				return nil, err
			}
			if redisPassword == "" {
				logger.Error("redis password is empty")
				return c, nil
			}
			_, err = c.Do("AUTH", redisPassword)
			if err != nil {
				c.Close()
			}
			return c, err
		},
		TestOnBorrow: func(c r.Conn, t time.Time) error {
			_, err := c.Do("PING")
			if err != nil {
				logger.Errorw("ping redis server error", "err", err)
			}
			return err
		},
	}
}

func GetPool() *r.Pool {
	return redisPool
}

/*
 	Set redis string value
		Paramters
			key: redis key
*/
func SetStringValue(key string, value string, expire int) error {
	conn := redisPool.Get()
	defer conn.Close()
	if expire < 0 {
		_, err := conn.Do("SET", key, value)
		return err
	}
	_, err := conn.Do("SETEX", key, expire, value)
	return err
}

/*
	Set redis bytes value
		Paramters
			key: redis key
*/
func SetBytesValue(key string, value []byte, expire int) error {
	conn := redisPool.Get()
	defer conn.Close()
	if expire < 0 {
		_, err := conn.Do("SET", key, value)
		return err
	}
	_, err := conn.Do("SETEX", key, expire, value)
	return err
}

/*
	Set redis interface value
		Paramters
			key: redis key
*/
func SetInterfaceValue(key string, value interface{}, expire int) error {
	conn := redisPool.Get()
	defer conn.Close()
	if expire < 0 {
		_, err := conn.Do("SET", key, value)
		return err
	}
	_, err := conn.Do("SETEX", key, expire, value)
	return err
}

/*
 	Set redis integer value
		Paramters
			key: redis key
*/
func SetIntValue(key string, value int64, expire int) error {
	conn := redisPool.Get()
	defer conn.Close()
	if expire <= 0 {
		_, err := conn.Do("SET", key, value)
		return err
	}
	_, err := conn.Do("SETEX", key, expire, value)
	return err
}

/*
	Get redis byte array format data
		Paramters
			key: redis key
*/
func GetBytes(key string) ([]byte, error) {
	conn := redisPool.Get()
	defer conn.Close()
	data, err := r.Bytes(conn.Do("GET", key))
	return data, err
}

/*
	Get redis string format data
		Paramters
			key: redis key
*/
func GetString(key string) (string, error) {
	conn := redisPool.Get()
	defer conn.Close()
	data, err := r.String(conn.Do("GET", key))
	return data, err
}

/*
	Get redis integer format data
		Paramters
			key: redis key
*/
func GetInt(key string) (int64, error) {
	conn := redisPool.Get()
	defer conn.Close()
	return r.Int64(conn.Do("GET", key))
}

/*
	Get redis key TTL
		Paramters
			key: redis key
*/
func GetTTL(key string) (int, error) {
	conn := redisPool.Get()
	defer conn.Close()
	return r.Int(conn.Do("TTL", key))
}

/*
	Delete redis data
		Paramters
			key: redis key
*/
func DelKey(key string) error {
	conn := redisPool.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", key)
	return err
}

/*
	Set redis array string data
		Paramters
			key: redis key
			data: array data
*/
func SetArrayStringData(key string, data []string, expire int) error {
	conn := redisPool.Get()
	defer conn.Close()
	_, err := conn.Do("DEL", key)
	if err != nil {
		return err
	}
	for _, d := range data {
		_, err := conn.Do("LPUSH", key, d)
		if err != nil {
			return err
		}
	}
	if expire >= 0 {
		_, err = conn.Do("EXPIRE", key, expire)
	}
	return err
}

/*
	Get redis array string data
*/
func GetArrayStringData(key string) ([]string, error) {
	conn := redisPool.Get()
	defer conn.Close()
	return r.Strings(conn.Do("LRANGE", key, 0, -1))
}
