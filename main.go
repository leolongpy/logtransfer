package main

import (
	"github.com/go-ini/ini"
	"log"
	"logtransfer/es"
	"logtransfer/kafka"
	"logtransfer/model"
)

func main() {
	var cfg = new(model.Config)
	err := ini.MapTo(cfg, "./config/logtransfer.ini")
	if err != nil {
		log.Printf("load config failed,err:%v\n", err)
		panic(err)
	}
	log.Println("load config success")
	err = es.Init(cfg.ESConf.Address, cfg.ESConf.Index, cfg.ESConf.GoNum, cfg.ESConf.MaxSize)
	if err != nil {
		log.Printf("Init es failed,logerr:%v\n", err)
		panic(err)
	}
	log.Println("Init ES success")

	err = kafka.Init([]string{cfg.KafkaConf.Address}, cfg.KafkaConf.Topic)
	if err != nil {
		log.Printf("connect to kafka failed,err:%v\n", err)
		panic(err)
	}
	log.Println("Init kafka success")

	select {}

}
