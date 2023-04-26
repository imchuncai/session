package session

import (
	"math/rand"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestGet(t *testing.T) {
	type Data struct {
		Int    int
		String string
	}

	rds := redis.NewClient(&redis.Options{
		Addr:         "localhost:8379",
		Password:     "",
		DB:           0,
		MaxIdleConns: 0,
	})
	newData := func(sessionID string) interface{} {
		return Data{rand.Int(), sessionID}
	}
	ses := New(rds, time.Second*5, "session_test", newData)

	first := new(Data)
	firstID, err := ses.Get("not_valid", first, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	second := new(Data)
	secondID, err := ses.Get(firstID, second, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	if firstID != secondID {
		t.Fatalf("want id: %s got %s", secondID, firstID)
	}

	if *first != *second {
		t.Fatalf("want data: %v got %v", first, second)
	}
}
