package main

import (
	"github.com/go-redis/redis"
	"github.com/justjack555/redisWC/pkg/redisWC"
	"log"
	"os"
)

func getRedisConn() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
}

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

func testWordCount(redisDb *redis.Client) {
	f := getFile()
	defer closeFile(f)

	rc := redisWC.New(redisDb, f)
	rc.StoreWordCounts()
}

func testMergeStreams(redisDb *redis.Client) {

	rc := redisWC.New(redisDb, nil)

	rc.SpawnStreams(2)
	rc.PrintInfiniteStreamLen()
}

func main(){
	redisDb := getRedisConn()

	testWordCount(redisDb)
	testMergeStreams(redisDb)


}