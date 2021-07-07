package main

import (
	"time"

	"github.com/katakurin/canal/protobuf/entry"
	"google.golang.org/protobuf/proto"

	"github.com/katakurin/canal"
	log "github.com/sirupsen/logrus"
)

func main() {
	connector, err := canal.NewClient("127.0.0.1:11111", "test")
	if err != nil {
		log.Fatal(err)
	}
	err = connector.Subscribe(".*\\\\..*")
	if err != nil {
		log.Fatal(err)
	}
	// 读取canal
	go func() {
		for {
			message, err := connector.GetWithOutAck(10, -1)
			if err != nil {
				log.Println(err)
				continue
			}
			batchID := message.ID
			if batchID == -1 || len(message.Entries) <= 0 {
				time.Sleep(1000 * time.Millisecond)
				continue
			}
			log.Info(message.ID)
			if !message.Raw {
				for _, v := range message.Entries {
					log.Info(v.String())
					change := entry.RowChange{}
					proto.Unmarshal(v.GetStoreValue(), &change)
					log.Info(change.String())
				}
			}

		}
	}()

	select {}
}
