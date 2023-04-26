package session

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/gob"
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

type session struct {
	redis     *redis.Client
	timeout   time.Duration
	keyPrefix string
	incrKey   string
	new       func(sessionID string) interface{}
}

// keyPrefix is session key prefix in redis.
// new should not return pointer.
func New(redis *redis.Client, timeout time.Duration, keyPrefix string, new func(sessionID string) interface{}) session {
	return session{redis, timeout, keyPrefix, keyPrefix + "incr", new}
}

func (s session) valid(id string) bool {
	return strings.HasPrefix(id, s.keyPrefix) && !strings.HasPrefix(id, s.incrKey)
}

func decode(from string, to interface{}) error {
	return gob.NewDecoder(bytes.NewBufferString(from)).Decode(to)
}

func encode(from interface{}) ([]byte, error) {
	var to bytes.Buffer
	err := gob.NewEncoder(&to).Encode(from)
	if err != nil {
		return nil, err
	}
	return to.Bytes(), err
}

// the value underlying sessionData must be a pointer to the correct type
func (s session) Get(id string, sessionData interface{}, ttl time.Duration) (newID string, err error) {
	if s.valid(id) {
		var v, err = s.get(id, ttl)
		if err == nil {
			return id, decode(v, sessionData)
		}
		if err != redis.Nil {
			return "", err
		}
	}
	id, err = s.newID()
	if err != nil {
		return "", err
	}
	reflect.ValueOf(sessionData).Elem().Set(reflect.ValueOf(s.new(id)))
	return id, s.Set(id, sessionData, ttl)
}

func (s session) newID() (string, error) {
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

func (s session) Set(id string, data interface{}, ttl time.Duration) error {
	var ctx, cancel = s.getTimeoutCtx()
	defer cancel()
	var val, err = encode(data)
	if err != nil {
		return err
	}
	return s.redis.SetEx(ctx, id, val, ttl).Err()
}

func (s session) get(id string, ddl time.Duration) (string, error) {
	var ctx, cancel = s.getTimeoutCtx()
	defer cancel()
	const script = "redis.call('expire',KEYS[1],ARGV[1]) return redis.call('get',KEYS[1])"
	return s.redis.Eval(ctx, script, []string{id}, int(ddl.Seconds())).Text()
}

func (s session) incr() (int, error) {
	var ctx, cancel = s.getTimeoutCtx()
	defer cancel()
	const script = "local v=redis.call('incr',KEYS[1]) if v>4000000000 then redis.call('del',KEYS[1]) end return v"
	return s.redis.Eval(ctx, script, []string{s.incrKey}).Int()
}

func (s session) Del(id string) error {
	if !s.valid(id) {
		return nil
	}
	var ctx, cancel = s.getTimeoutCtx()
	defer cancel()
	return s.redis.Del(ctx, id).Err()
}

func (s session) Expire(id string, ttl time.Duration) error {
	if !s.valid(id) {
		return nil
	}
	var ctx, cancel = s.getTimeoutCtx()
	defer cancel()
	return s.redis.Expire(ctx, id, ttl).Err()
}
