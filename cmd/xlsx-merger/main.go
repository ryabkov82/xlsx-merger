package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/ryabkov82/xlsx-merger/internal/config"
	"github.com/ryabkov82/xlsx-merger/internal/merger"
)

type Output struct {
	Success     bool     `json:"success"`
	OutputFiles []string `json:"output_files,omitempty"`
	Error       string   `json:"error,omitempty"`
}

func main() {

	cfg, err := config.ParseFlags()
	if err != nil {
		emitJSON(Output{
			Success: false,
			Error:   fmt.Sprintf("Ошибка конфигурации: %v", err),
		})
		return
	}

	m := merger.NewStreamMerger()
	outputFiles, err := m.MergeFiles(cfg)
	if err != nil {
		emitJSON(Output{
			Success: false,
			Error:   fmt.Sprintf("Ошибка объединения: %v", err),
		})
		return
	}

	emitJSON(Output{
		Success:     true,
		OutputFiles: outputFiles,
	})

}

func emitJSON(out Output) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ") // для красивого вывода (опционально)
	if err := enc.Encode(out); err != nil {
		log.Fatalf("Ошибка вывода JSON: %v", err)
	}
}
