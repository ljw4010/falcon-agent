package g

import (
	"encoding/json"
	"time"

	"log"

	"github.com/Shopify/sarama"
	"github.com/open-falcon/falcon-plus/common/model"
)

var (
	SYS_TOPIC string = "SYS_MONITOR"
)

func SendKafkaMetrics(metrics []*model.MetricValue) {
	var kafkaAddrs []string = Config().Kafka.Addrs
	SyncProducer(kafkaAddrs, metrics)
}

//同步消息模式
func SyncProducer(KfkAddr []string, metrics []*model.MetricValue) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Retry.Backoff = time.Duration(Config().Kafka.Timeout) * time.Second
	config.Producer.Retry.Max = Config().Kafka.Interval
	config.Producer.Return.Errors = true
	config.Producer.Timeout = time.Duration(Config().Kafka.Timeout) * time.Second
	p, err := sarama.NewSyncProducer(KfkAddr, config)
	if err != nil {
		log.Printf("sarama.NewSyncProducer err, message=%s \n", err)
		return
	}
	defer p.Close()

	metric, err := json.Marshal(metrics)
	if err != nil {
		log.Printf("marshal[%+v] failed fail: %v", metrics, err)
	}
	msg := &sarama.ProducerMessage{
		Topic: SYS_TOPIC,
		Value: sarama.StringEncoder(metric),
	}
	part, offset, err := p.SendMessage(msg)
	if err != nil {
		log.Printf("send message(%s) err=%s \n", metric, err)
	} else {
		log.Printf("topic[%s] send success，partition=%d, offset=%d \n", SYS_TOPIC, part, offset)
	}
}
