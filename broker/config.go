package broker

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Config struct {
	DataDir                        string
	Port                           int
	SegmentMaxSizeBytes            int
	SegmentMaxSizeMessages         int
	MessageRetentionSec            int
	UnloadMessagesLagSec           int
	StorageFlushingToFilePeriodSec int
	StorageHousekeepingPeriodSec   int
}

func ReadConfig() (*Config, error) {
	pflag.String("data-dir", "./data", "directory where to store all broker data")
	pflag.String("port", "8010", "port to listen API commands")
	pflag.Int("segment-max-size-bytes", 10*1024*1024, "maximum segment size in bytes")
	pflag.Int("segment-max-size-messages", 100000, "maximum messages in segment")
	pflag.Int("storage-message-retention-sec", 60*24, "message retention in seconds (based on message timestamp filed)")
	pflag.Int("segment-unload-messages-lag-sec", 60, "duration after which segment messages will be unloaded from RAM (if no reads occur)")
	pflag.Int("storage-flushind-pediod-sec", 60, "frequency of persistently storaging of new messages")
	pflag.Int("storage-housekeeping-pediod-sec", 10, "frequency of housekeeping routines will be called")
	pflag.Parse()

	c := viper.New()
	if err := c.BindPFlags(pflag.CommandLine); err != nil {
		return nil, err
	}
	c.AutomaticEnv()

	config := &Config{}

	config.DataDir = c.GetString("data-dir")
	config.Port = c.GetInt("port")
	config.SegmentMaxSizeBytes = c.GetInt("segment-max-size-bytes")
	config.SegmentMaxSizeMessages = c.GetInt("segment-max-size-messages")

	config.MessageRetentionSec = c.GetInt("storage-message-retention-sec")
	config.UnloadMessagesLagSec = c.GetInt("segment-unload-messages-lag-sec")
	config.StorageFlushingToFilePeriodSec = c.GetInt("storage-flushind-pediod-sec")
	config.StorageHousekeepingPeriodSec = c.GetInt("storage-housekeeping-pediod-sec")
	return config, nil
}

func (c *Config) LogConfig() {
	log.Info("Config:")
	log.Info("    data-dir: ", c.DataDir)
	log.Info("    port: ", c.Port)
	log.Info("    segment-max-size-bytes: ", c.SegmentMaxSizeBytes)
	log.Info("    segment-max-size-messages: ", c.SegmentMaxSizeMessages)
	log.Info("    storage-message-retention-sec: ", c.MessageRetentionSec)
	log.Info("    segment-unload-messages-lag-sec: ", c.UnloadMessagesLagSec)
	log.Info("    storage-flushind-pediod-sec: ", c.StorageFlushingToFilePeriodSec)
	log.Info("    storage-housekeeping-pediod-sec: ", c.StorageHousekeepingPeriodSec)
}
