package broker

import (
	"os"
	"path"

	"github.com/dmitryikh/tube"
	log "github.com/sirupsen/logrus"
)

type Broker struct {
	TopicsManager     *TopicsManager
	ConsumersRegistry *ConsumersRegistry
	config            Config
	consumedSeqThread *tube.PeriodicThread
	prevSeqSet        tube.SeqSet
}

func NewBroker(c Config) (*Broker, error) {
	if !tube.IsDir(c.DataDir) {
		log.Infof("Creating data directory \"%s\"", c.DataDir)
		err := os.MkdirAll(c.DataDir, 0777)
		if err != nil {
			return nil, err
		}
	}
	consumerRegFilepath := path.Join(c.DataDir, consumerRegistryFilename)
	if !tube.IsRegularFile(consumerRegFilepath) {
		cReg := NewConsumersRegistry()
		err := cReg.SaveToFile(consumerRegFilepath)
		if err != nil {
			return nil, err
		}
		log.Infof("Creating ConsumerRegistry in \"%s\"", consumerRegistryFilename)
	}
	log.Info("Loading ConsumerRegistry")
	consumerReg, err := ConsumerRegistryFromFile(consumerRegFilepath)
	if err != nil {
		return nil, err
	}

	topicsManager, err := NewTopicsManager(&c)
	if err != nil {
		return nil, err
	}

	broker := &Broker{
		TopicsManager:     topicsManager,
		ConsumersRegistry: consumerReg,
		config:            c,
		prevSeqSet:        tube.NewSeqSet(),
	}

	// fill TopicsManager's consumedSeq with actual data from ConsumersRegistry
	consumedSeqs := consumerReg.GetAllTopicsSeqs()
	for topicName, seq := range consumedSeqs {
		topicsManager.SetConsumedSeq(topicName, seq)
	}

	broker.consumedSeqThread = tube.NewPeriodicThread(
		broker.setConsumedSeqLoopInternal,
		5,
	)
	return broker, nil
}

func (b *Broker) Shutdown() error {
	b.consumedSeqThread.Stop()
	b.consumedSeqThread.Join()

	var err1 error
	var err2 error
	err1 = b.ConsumersRegistry.SaveToFile(
		path.Join(b.config.DataDir, consumerRegistryFilename),
	)
	if err1 != nil {
		log.Errorf("Error while saving ConsumerRegistry: %s", err1)
	}
	err2 = b.TopicsManager.Shutdown()
	if err2 != nil {
		log.Errorf("Error while shutting down TopicsManager: %s", err2)
	}
	if err1 != nil {
		return err1
	}
	if err2 != nil {
		return err2
	}
	return nil
}

func (b *Broker) setConsumedSeqLoopInternal() {
	seqSet := b.ConsumersRegistry.GetAllTopicsSeqs()

	if !seqSet.Equals(b.prevSeqSet) {
		err := b.ConsumersRegistry.SaveToFile(path.Join(b.config.DataDir, consumerRegistryFilename))
		if err != nil {
			log.Error("Failed to save ConsumerRegistry: ", err)
			return
		}
		for topicName, seq := range seqSet {
			prevSeq, isFound := b.prevSeqSet[topicName]
			if isFound && prevSeq == seq {
				continue
			}
			b.TopicsManager.SetConsumedSeq(topicName, seq)
		}
		b.prevSeqSet = seqSet
	}

}
