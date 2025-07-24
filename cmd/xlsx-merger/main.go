package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/ryabkov82/xlsx-merger/internal/config"
	"github.com/ryabkov82/xlsx-merger/internal/merger"
)

type Output struct {
	Success     bool     `json:"success"`
	OutputFiles []string `json:"output_files,omitempty"`
	Error       string   `json:"error,omitempty"`
	Duration    string   `json:"duration"`
	RowCount    int64    `json:"row_count,omitempty"`
}

func main() {

	start := time.Now()

	cfg, err := config.ParseFlags()
	if err != nil {
		emitJSON(Output{
			Success:  false,
			Error:    fmt.Sprintf("Ошибка конфигурации: %v", err),
			Duration: time.Since(start).String(),
		})
		return
	}

	m := merger.NewStreamMerger()
	outputFiles, RowCount, err := m.MergeFiles(cfg)
	if err != nil {
		emitJSON(Output{
			Success:  false,
			Error:    fmt.Sprintf("Ошибка объединения: %v", err),
			Duration: time.Since(start).String(),
		})
		return
	}

	emitJSON(Output{
		Success:     true,
		OutputFiles: outputFiles,
		RowCount:    RowCount,
		Duration:    time.Since(start).String(),
	})

}

func emitJSON(out Output) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ") // для красивого вывода (опционально)
	if err := enc.Encode(out); err != nil {
		log.Fatalf("Ошибка вывода JSON: %v", err)
	}
}
