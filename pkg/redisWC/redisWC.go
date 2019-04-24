package redisWC

import (
	"bufio"
	"github.com/go-redis/redis"
	"log"
	"os"
)

type RedisWC struct {
	redisConn *redis.Client
	file *os.File
}

/**
	Initialize new Redis Word Counter
 */
func New(redisConn *redis.Client, file *os.File) *RedisWC {
	rc := new(RedisWC)

	rc.redisConn = redisConn
	rc.file = file

	return rc
}

func (rc *RedisWC) incr(word string) error {
	res, err := rc.redisConn.Incr(word).Result()
	if err != nil {
		log.Println("incr(): Unable to increment key: ", word, "; Incrementing error count")
		return err
	}

	log.Println("incr(): Successfully added word: ", word, " with res: ", res)
	return nil
}

func (rc *RedisWC) scanAndStore() (int, int) {
	sc := bufio.NewScanner(rc.file)
	sc.Split(bufio.ScanWords)
	errCount := 0
	i := 0

	for sc.Scan() {
		word := sc.Text()
		err := rc.incr(word)
		if err != nil {
			errCount++
		}

		i++
	}

	if err := sc.Err(); err != nil {
		log.Println("scanAndStore(): Error reading input:", err)
	}

	return i, errCount
}

func (rc *RedisWC) StoreWordCounts() {
	count, errs := rc.scanAndStore()
	log.Println("StoreWordCounts(): Stored ", count - errs, " words with ", errs, " errors.")
}
