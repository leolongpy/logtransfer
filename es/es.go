package es

import (
	"context"
	"github.com/olivere/elastic/v7"
	"log"
)

type ESClient struct {
	client      *elastic.Client
	index       string
	logDataChan chan interface{}
}

var esClient *ESClient

func Init(addr, index string, goroutineNum, maxSize int) (err error) {
	client, err := elastic.NewClient(elastic.SetURL("http://" + addr))
	if err != nil {
		panic(err)
	}
	esClient = &ESClient{
		client:      client,
		index:       index,
		logDataChan: make(chan interface{}, maxSize),
	}
	log.Println("connect to es succsess")
	for i := 0; i < goroutineNum; i++ {
		go sendToES()
	}
	return
}

func sendToES() {
	for m1 := range esClient.logDataChan {
		put1, err := esClient.client.Index().Index(esClient.index).BodyJson(m1).Do(context.Background())
		if err != nil {
			panic(err)
		}
		log.Printf("Indexed user %s to index %s, type %s\n", put1.Id, put1.Index, put1.Type)
	}
}

func PutLogData(msg interface{}) {
	esClient.logDataChan <- msg
}
