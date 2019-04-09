package kafka

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
)

type avroConsumer struct {
	SchemaRegistryClient *CachedSchemaRegistryClient
	callbacks            ConsumerCallbacks
	ready                chan bool
	cleanup              chan bool
	consumerGroup        sarama.ConsumerGroup
	meta                 string
}

type ConsumerCallbacks struct {
	OnDataReceived func(msg Message) bool // return true to continue
	OnError        func(err error) bool   // return true to continue on error
}

type Message struct {
	SchemaId  int
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Value     string
}

// avroConsumer is a basic consumer to interact with schema registry, avro and kafka
func NewAvroConsumer(kafkaServers []string, kafkaVersion sarama.KafkaVersion, schemaRegistryServers []string,
	topic string, groupId string, callbacks ConsumerCallbacks, meta string) (*avroConsumer, error) {
	// // init (custom) config, enable errors and notifications
	// config := cluster.NewConfig()
	// config.Consumer.Return.Errors = true
	// config.Group.Return.Notifications = true
	// //read from beginning at the first time
	// config.Consumer.Offsets.Initial = sarama.OffsetOldest
	// topics := []string{topic}
	// consumer, err := cluster.NewConsumer(kafkaServers, groupId, topics, config)
	// if err != nil {
	// 	return nil, err
	// }

	config := sarama.NewConfig()
	config.Version = kafkaVersion

	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	schemaRegistryClient := NewCachedSchemaRegistryClient(schemaRegistryServers)

	consumer := &avroConsumer{
		SchemaRegistryClient: schemaRegistryClient,
		callbacks:            callbacks,
		ready:                make(chan bool, 0),
		cleanup:              make(chan bool, 0),
		meta:                 meta,
	}

	ctx := context.Background()
	consumerGroup, err := sarama.NewConsumerGroup(kafkaServers, groupId, config)
	if err != nil {
		return nil, err
	}

	consumer.consumerGroup = consumerGroup

	go func() {
		for {
			err := consumerGroup.Consume(ctx, []string{topic}, consumer)
			if err != nil {
				panic(err)
			}
		}
	}()

	<-consumer.ready // Await till the consumer has been set up

	return consumer, nil
}

//GetSchemaId get schema id from schema-registry service
func (ac *avroConsumer) GetSchema(id int) (*goavro.Codec, error) {
	codec, err := ac.SchemaRegistryClient.GetSchema(id)
	if err != nil {
		return nil, err
	}
	return codec, nil
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (ac *avroConsumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(ac.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (ac *avroConsumer) Cleanup(sarama.ConsumerGroupSession) error {
	close(ac.cleanup)

	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (ac *avroConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	//log.Printf("%v consuming from %v[%v]", ac.meta, claim.Topic(), claim.Partition())

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {

		msg, err := ac.ProcessAvroMsg(message)
		if err != nil {
			if !ac.callbacks.OnError(err) {
				return err
			}
		}
		if ac.callbacks.OnDataReceived != nil {
			if !ac.callbacks.OnDataReceived(msg) {
				return errors.New("OnDataReceived decided to abort")
			}
		}

		session.MarkMessage(message, "")
	}

	return nil
}

func (ac *avroConsumer) ProcessAvroMsg(m *sarama.ConsumerMessage) (Message, error) {
	schemaId := binary.BigEndian.Uint32(m.Value[1:5])
	codec, err := ac.GetSchema(int(schemaId))
	if err != nil {
		return Message{}, err
	}
	// Convert binary Avro data back to native Go form
	native, _, err := codec.NativeFromBinary(m.Value[5:])
	if err != nil {
		return Message{}, err
	}

	// Convert native Go form to textual Avro data
	textual, err := codec.TextualFromNative(nil, native)

	if err != nil {
		return Message{}, err
	}
	msg := Message{int(schemaId), m.Topic, m.Partition, m.Offset, string(m.Key), string(textual)}
	return msg, nil
}

func (ac *avroConsumer) Close() {
	ac.consumerGroup.Close()
}

func (ac *avroConsumer) Wait() {
	<-ac.cleanup
}
