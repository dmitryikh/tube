package storage

import (
	"fmt"
	"io/ioutil"
	"path"
	"strconv"

	"github.com/dmitryikh/tube"
)

func segmentFilename(header *SegmentHeader) string {
	return fmt.Sprintf("%d_%d.segment", header.SeqMin, header.SeqMax)
}

func isSegmentFile(filePath string) bool {
	return segmentFilePathPattern.MatchString(filePath)
}

func minMaxSeqsFromSegmentFilename(filepath string) (uint64, uint64) {
	filename := path.Base(filepath)
	groups := segmentFilePathPattern.FindStringSubmatch(filename)
	if len(groups) != 3 {
		return tube.UnsetSeq, tube.UnsetSeq
	}
	seqMin, _ := strconv.ParseUint(groups[1], 10, 64)
	seqMax, _ := strconv.ParseUint(groups[2], 10, 64)
	return seqMin, seqMax
}

func isTopicDir(dirPath string) bool {
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return false
	}
	for _, file := range files {
		if isSegmentFile(file.Name()) {
			return true
		}
	}
	return false
}
