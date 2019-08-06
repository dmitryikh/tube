package broker

import (
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

type Config struct {
	DataDir                string
	SegmentMaxSizeBytes    int
	SegmentMaxSizeMessages int
}

func ReadConfig() (*Config, error) {
	pflag.String("data-dir", "./data", "directory where to store all broker data")
	pflag.Int("segment-max-size-bytes", 10*1024*1024, "maximum segment size in bytes")
	pflag.Int("segment-max-size-messages", 100000, "maximum messages in segment")
	pflag.Parse()

	c := viper.New()
	if err := c.BindPFlags(pflag.CommandLine); err != nil {
		return nil, err
	}
	c.AutomaticEnv()

	config := &Config{}

	config.DataDir = c.GetString("data-dir")
	config.SegmentMaxSizeBytes = c.GetInt("segment-max-size-bytes")
	config.SegmentMaxSizeMessages = c.GetInt("segment-max-size-messages")
	return config, nil
}
