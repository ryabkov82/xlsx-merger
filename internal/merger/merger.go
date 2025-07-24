package merger

import (
	"github.com/ryabkov82/xlsx-merger/internal/config"
)

type FileMerger interface {
	MergeFiles(cfg *config.Config) ([]string, int64, error)
}

type BaseMerger struct {
	Headers      []string
	MaxColWidths map[int]int
}

// Init инициализирует базовые поля
func (bm *BaseMerger) Init() {
	bm.MaxColWidths = make(map[int]int)
	bm.Headers = make([]string, 0)
}

// AnalyzeSample анализирует пример данных для определения ширины колонок
