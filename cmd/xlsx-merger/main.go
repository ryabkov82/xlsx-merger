package main

import (
	"fmt"
	"log"

	"github.com/ryabkov82/xlsx-merger/internal/config"
	"github.com/ryabkov82/xlsx-merger/internal/merger"
)

func main() {

	cfg, err := config.ParseFlags()
	if err != nil {
		log.Fatalf("Ошибка конфигурации: %v", err)
	}

	m := merger.NewStreamMerger()
	outputFiles, err := m.MergeFiles(cfg)
	if err != nil {
		log.Fatalf("Ошибка объединения: %v", err)
	}

	fmt.Printf("Файлы успешно объединены в %s\n", outputFiles)
}
