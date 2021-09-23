package kafka

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"log"
	"logtransfer/es"
)

func Init(addr []string, topic string) (err error) {
	consumer, err := sarama.NewConsumer(addr, nil)
	if err != nil {
		log.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	//拿到topic下所有的分区
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		log.Printf("fail to get list of partition:err%v\n", err)
		return
	}
	for partition := range partitionList {
		var pc sarama.PartitionConsumer
		pc, err = consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			log.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
			return
		}
		log.Println("start to consume")
		go func(partitionConsumer sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				log.Println(msg.Topic, string(msg.Value))
				var m1 map[string]interface{}
				err = json.Unmarshal(msg.Value, &m1)
				if err != nil {
					log.Printf("unmarshal msg failed, err:%v\n", err)
					m2 := make(map[string]interface{})
					m2["content"] = string(msg.Value)
					es.PutLogData(m2)
					continue
				}

				es.PutLogData(m1)
			}
		}(pc)

	}
	return
}
