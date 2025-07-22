package config

import (
	"flag"
	"fmt"
	"path/filepath"
)

type Config struct {
	InputDir      string
	OutputPath    string
	SampleRows    int
	AddSourceFile bool
	HasHeaders    bool  // Флаг наличия заголовков в исходных файлах
	MaxRowPerFile int64 // максимальное количество строк в объединенном файле
}

func ParseFlags() (*Config, error) {

	cfg := &Config{}

	flag.StringVar(&cfg.InputDir, "dir", "", "папка с исходными XLSX файлами")
	flag.StringVar(&cfg.OutputPath, "out", "./merged.xlsx", "результирующий файл")
	flag.IntVar(&cfg.SampleRows, "sample", 1000, "количество анализируемых строк")
	flag.BoolVar(&cfg.AddSourceFile, "add-source", false, "добавлять колонку с именем файла")
	flag.BoolVar(&cfg.HasHeaders, "has-headers", false, "исходные файлы содержат заголовки")
	flag.Int64Var(&cfg.MaxRowPerFile, "max-row", 600000, "максимальное количество строк в объединенном файле")

	flag.Parse()

	if cfg.InputDir == "" {
		return nil, fmt.Errorf("необходимо указать папку с файлами через -dir")
	}

	// Нормализация путей
	cfg.InputDir = filepath.Clean(cfg.InputDir)
	cfg.OutputPath = filepath.Clean(cfg.OutputPath)

	return cfg, nil
}
