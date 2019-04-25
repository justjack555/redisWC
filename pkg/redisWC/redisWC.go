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

const (
	SONG_WORDS = "song_words"
)

func getFile() *os.File {
	if len(os.Args) != 2 {
		log.Fatalln("Usage: ./redis <file_path>")
	}

	path := os.Args[1]
	file, err := os.Open(path)
	if err != nil {
		log.Fatal("getFile(): Unable to open file ", err)
	}

	return file
}

func closeFile(f *os.File){
	if err := f.Close(); err != nil {
		log.Fatalln("Unable to close file: ", err)
	}
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
	_, err := rc.redisConn.Incr(word).Result()
	if err != nil {
		log.Println("incr(): Unable to increment key: ", word, "; Incrementing error count")
		return err
	}

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

func (rc *RedisWC) spawnStream(streamChan chan string, doneChan chan bool) {
	f := getFile()
	defer closeFile(f)

	rc.file = f

	sc := bufio.NewScanner(rc.file)
	sc.Split(bufio.ScanWords)

	for sc.Scan() {
		word := sc.Text()
		streamChan <- word
	}

	if err := sc.Err(); err != nil {
		log.Println("spawnStream(): Error reading input:", err)
	}

	doneChan <- true
}

func (rc *RedisWC) startMerger(streamChan chan string) {
	errCount := 0
	count := 0
	for v := range streamChan {
		_, err := rc.redisConn.LPush(SONG_WORDS, v).Result()
		if err != nil {
			log.Println("startMerger(): Error with LPush:", err)
			errCount++
		}

		count++
	}

	log.Println("startMerger(): Stored ", count - errCount, " words with ", errCount, " errors.")
}

func (rc *RedisWC) SpawnStreams(n int) {
	streamChan := make(chan string)
	doneChan := make(chan bool)

	for i := 0; i < n; i++ {
		log.Println("SpawnStreams(): Spawning ", i, "th stream")
		go rc.spawnStream(streamChan, doneChan)
	}

	go rc.startMerger(streamChan)

	for i := 0; i < n; i++ {
		<- doneChan
	}


	close(doneChan)
	close(streamChan)
}

func (rc *RedisWC) PrintInfiniteStreamLen()  {
	res, err := rc.redisConn.LLen(SONG_WORDS).Result()
	if err != nil {
		log.Println("startMerger(): Error with LLen:", err)
		return
	}
	log.Println("printInfiniteStreamLen(): Length of stream is: ", res)
}

func (rc *RedisWC) StoreWordCounts() {
	count, errs := rc.scanAndStore()
	log.Println("StoreWordCounts(): Stored ", count - errs, " words with ", errs, " errors.")
}
