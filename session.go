package session

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"io"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

type session struct {
	redis     *redis.Client
	timeout   time.Duration
	keyPrefix string
	incrKey   string
	dataFunc  SessionDataBuilder
}

// keyPrefix: key prefix in redis
func NewSession(redis *redis.Client, timeout time.Duration, keyPrefix string, dataFunc SessionDataBuilder) session {
	return session{redis, timeout, keyPrefix, keyPrefix + "incr", dataFunc}
}

type SessionDataBuilder interface {
	New(sessionID string) string
	Unmarshal(data string, v interface{}) error
	Marshal(v interface{}) (string, error)
}

func (s session) valid(id string) bool {
	return strings.HasPrefix(id, s.keyPrefix) && !strings.HasPrefix(id, s.incrKey)
}

func (s session) Get(id string, sessionData interface{}, ttl time.Duration) (newID string, err error) {
	if s.valid(id) {
		var v, err = s.get(id, ttl)
		if err == nil {
			return id, s.dataFunc.Unmarshal(v, &sessionData)
		}
		if err != redis.Nil {
			return "", err
		}
	}
	id, err = s.newSessionID()
	if err != nil {
		return "", err
	}
	var v = s.dataFunc.New(id)
	err = s.dataFunc.Unmarshal(v, &sessionData)
	if err != nil {
		return "", err
	}
	return id, s.Set(id, sessionData, ttl)
}

func (s session) newSessionID() (string, error) {
	var incr, err = s.incr()
	if err != nil {
		return "", err
	}
	var b = make([]byte, 64+8+4)
	io.ReadFull(rand.Reader, b[:64])
	binary.BigEndian.PutUint64(b[64:64+8], uint64(time.Now().UnixNano()))
	binary.BigEndian.PutUint32(b[64+8:], uint32(incr))
	return s.keyPrefix + base64.URLEncoding.EncodeToString(b), nil
}

func (s session) getTimeoutCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), s.timeout)
}

func (s session) Set(key string, data interface{}, ttl time.Duration) error {
	var ctx, cancel = s.getTimeoutCtx()
	defer cancel()
	var val, err = s.dataFunc.Marshal(data)
	if err != nil {
		return err
	}
	return s.redis.SetEX(ctx, key, val, ttl).Err()
}

func (s session) get(key string, ddl time.Duration) (string, error) {
	var ctx, cancel = s.getTimeoutCtx()
	defer cancel()
	const script = "redis.call('expire',KEYS[1],ARGV[1]) return redis.call('get',KEYS[1])"
	return s.redis.Eval(ctx, script, []string{key}, int(ddl.Seconds())).Text()
}

func (s session) incr() (int, error) {
	var ctx, cancel = s.getTimeoutCtx()
	defer cancel()
	const script = "local v=redis.call('incr',KEYS[1]) if v>4000000000 then redis.call('del',KEYS[1]) end return v"
	return s.redis.Eval(ctx, script, []string{s.incrKey}, nil).Int()
}

func (s session) Del(key string) error {
	if !s.valid(key) {
		return nil
	}
	var ctx, cancel = s.getTimeoutCtx()
	defer cancel()
	return s.redis.Del(ctx, key).Err()
}
