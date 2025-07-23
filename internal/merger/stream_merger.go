package merger

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ryabkov82/xlsx-merger/internal/config"
	"github.com/xuri/excelize/v2"
)

type StreamMerger struct {
	BaseMerger
	// Специфичные для потоковой обработки поля
	RowStyles    []int
	HeaderStyles []int
	ValueTypes   []excelize.CellType
	StyleCache   map[string]int
	UseTemplate  bool
	Cfg          *config.Config
	StreamWriter *excelize.StreamWriter
	RowCounter   int64
	HeightHeader float64
	Sheet        string
	OutFile      *excelize.File
	PartCounter  int
	OutputFiles  []string
}

func NewStreamMerger() FileMerger {
	sm := &StreamMerger{}
	sm.BaseMerger.Init() // Инициализация базовой части
	return sm
}

func (sm *StreamMerger) newOutput() error {
	// Завершение текущего файла
	if sm.OutFile != nil {
		if err := sm.StreamWriter.Flush(); err != nil {
			return fmt.Errorf("ошибка финального flush: %v", err)
		}
		fileName := fmt.Sprintf("%s_part%d.xlsx", strings.TrimSuffix(sm.Cfg.OutputPath, ".xlsx"), sm.PartCounter)
		if err := sm.OutFile.SaveAs(fileName); err != nil {
			return fmt.Errorf("ошибка сохранения файла: %v", err)
		}
		_ = sm.OutFile.Close()
		sm.OutputFiles = append(sm.OutputFiles, fileName)
		sm.PartCounter++
	}

	var err error
	sm.OutFile, err = excelize.OpenFile(sm.Cfg.TemplatePath)
	if err != nil {
		return fmt.Errorf("ошибка открытия шаблона: %v", err)
	}
	sheetList := sm.OutFile.GetSheetList()
	if len(sheetList) == 0 {
		return fmt.Errorf("шаблон пустой, нет листов")
	}
	sm.Sheet = "merged"
	sm.OutFile.NewSheet(sm.Sheet)

	// Копируем ширину колонок из первого листа шаблона
	for colIdx := 1; colIdx <= len(sm.Headers); colIdx++ {
		colName, _ := excelize.ColumnNumberToName(colIdx)
		width, err := sm.OutFile.GetColWidth(sheetList[0], colName)
		if err == nil {
			sm.OutFile.SetColWidth(sm.Sheet, colName, colName, width)
		}
	}

	sm.StreamWriter, err = sm.OutFile.NewStreamWriter(sm.Sheet)
	if err != nil {
		return fmt.Errorf("ошибка создания StreamWriter: %v", err)
	}

	sm.OutFile.DeleteSheet(sheetList[0])
	sm.RowCounter = 0

	return nil
}

func (sm *StreamMerger) processInputFile(path string) error {
	f, err := excelize.OpenFile(path)
	if err != nil {
		return fmt.Errorf("ошибка открытия файла %s: %v", path, err)
	}
	defer f.Close()

	sheetListSrc := f.GetSheetList()
	if len(sheetListSrc) == 0 {
		return nil
	}
	sheetSrc := sheetListSrc[0]

	rows, err := f.Rows(sheetSrc)
	if err != nil {
		return fmt.Errorf("ошибка чтения строк из %s: %v", path, err)
	}

	rowInFile := 1
	if sm.Cfg.HasHeaders {
		rows.Next()
		rowInFile++
	}

	for rows.Next() {
		if sm.Cfg.MaxRowPerFile > 0 && sm.RowCounter >= sm.Cfg.MaxRowPerFile {
			if err := sm.newOutput(); err != nil {
				return err
			}
		}

		if sm.RowCounter == 0 {
			if sm.Cfg.HasHeaders && len(sm.Headers) > 0 {
				sm.RowCounter = 1
				headerRow := make([]interface{}, len(sm.Headers))
				for i, h := range sm.Headers {
					headerRow[i] = excelize.Cell{
						Value:   h,
						StyleID: sm.HeaderStyles[i],
					}
				}

				cell := fmt.Sprintf("A%d", sm.RowCounter)
				if err := sm.StreamWriter.SetRow(cell, headerRow, excelize.RowOpts{Height: sm.HeightHeader}); err != nil {
					return fmt.Errorf("ошибка записи заголовков: %v", err)
				}
				sm.RowCounter++
			}
		}

		stringRow, err := rows.Columns()
		if err != nil {
			return fmt.Errorf("ошибка чтения строки: %v", err)
		}

		rowData := make([]interface{}, len(stringRow))
		for i, cellVal := range stringRow {
			styleID := 0
			if i < len(sm.RowStyles) {
				styleID = sm.RowStyles[i]
			}

			colName, _ := excelize.ColumnNumberToName(i + 1)
			cellRef := fmt.Sprintf("%s%d", colName, rowInFile)

			var valType excelize.CellType
			if sm.UseTemplate && i < len(sm.ValueTypes) {
				valType = sm.ValueTypes[i]
			} else {
				valType, _ = f.GetCellType(sheetSrc, cellRef)
			}

			var value interface{}
			switch valType {
			case excelize.CellTypeBool:
				value = (cellVal == "1" || strings.ToLower(cellVal) == "true")
			case excelize.CellTypeNumber:
				if n, err := strconv.ParseFloat(cellVal, 64); err == nil {
					value = n
					if !sm.UseTemplate {
						decimals := 0
						if parts := strings.Split(cellVal, "."); len(parts) == 2 {
							decimals = len(parts[1])
						}
						cacheKey := fmt.Sprintf("%d_%d", i, decimals)
						if cachedStyle, ok := sm.StyleCache[cacheKey]; ok {
							styleID = cachedStyle
						} else {
							sm.StyleCache[cacheKey] = styleID
						}
					}
				} else {
					value = cellVal
				}
			case excelize.CellTypeDate:
				value = cellVal
			default:
				value = cellVal
			}

			rowData[i] = excelize.Cell{
				Value:   value,
				StyleID: styleID,
			}
		}

		if sm.Cfg.AddSourceFile {
			rowData = append(rowData, filepath.Base(path))
		}

		//height, _ := f.GetRowHeight(sheetSrc, rowInFile)
		height := rows.GetRowOpts().Height
		cell := fmt.Sprintf("A%d", sm.RowCounter)
		if err := sm.StreamWriter.SetRow(cell, rowData, excelize.RowOpts{Height: height}); err != nil {
			return fmt.Errorf("ошибка записи строки: %v", err)
		}

		sm.RowCounter++
		rowInFile++
	}

	return nil
}

func (sm *StreamMerger) prepareTemplate() error {
	fTemplate, err := excelize.OpenFile(sm.Cfg.TemplatePath)
	if err != nil {
		return fmt.Errorf("ошибка открытия шаблона: %v", err)
	}
	defer fTemplate.Close()

	sheetList := fTemplate.GetSheetList()
	if len(sheetList) == 0 {
		return fmt.Errorf("шаблон пустой, нет листов")
	}
	sheet := sheetList[0]

	rows, err := fTemplate.Rows(sheet)
	if err != nil {
		return err
	}

	// Получение заголовков
	if len(sm.Headers) == 0 && rows.Next() {
		headers, _ := rows.Columns()
		if sm.Cfg.AddSourceFile {
			if sm.Cfg.HasHeaders {
				sm.Headers = append(headers, "SourceFile")
			} else {
				sm.Headers = append(headers, "")
			}
		} else {
			sm.Headers = headers
		}
		sm.HeightHeader = rows.GetRowOpts().Height
	}

	// Стили заголовков и первой строки данных
	sm.HeaderStyles = make([]int, len(sm.Headers))
	sm.RowStyles = make([]int, len(sm.Headers))
	// Определение типов данных
	sm.ValueTypes = make([]excelize.CellType, len(sm.Headers))

	for col := 1; col <= len(sm.Headers); col++ {
		cell1, _ := excelize.CoordinatesToCellName(col, 1)
		styleID1, _ := fTemplate.GetCellStyle(sheet, cell1)
		sm.HeaderStyles[col-1] = styleID1

		cell2, _ := excelize.CoordinatesToCellName(col, 2)
		styleID2, _ := fTemplate.GetCellStyle(sheet, cell2)
		sm.RowStyles[col-1] = styleID2

		t, err := fTemplate.GetCellType(sheet, cell2)
		if err != nil {
			t = excelize.CellTypeInlineString
		} else if t == excelize.CellTypeUnset {
			styleID, _ := fTemplate.GetCellStyle(sheet, cell2)
			style, _ := fTemplate.GetStyle(styleID)
			switch {
			case isDateFormat(style.NumFmt):
				t = excelize.CellTypeDate
			case isNumericFormat(style.NumFmt):
				t = excelize.CellTypeNumber
			default:
				t = excelize.CellTypeInlineString
			}
		}
		sm.ValueTypes[col-1] = t
	}

	// Кеш стилей, если не передан TemplatePath
	if !sm.UseTemplate {
		sm.StyleCache = make(map[string]int)
		tmplRows, err := fTemplate.Rows(sheet)
		if err != nil {
			return fmt.Errorf("ошибка чтения строк шаблона: %v", err)
		}
		rowIdx := 1
		for tmplRows.Next() && rowIdx <= sm.Cfg.SampleRows {
			values, err := tmplRows.Columns()
			if err != nil {
				continue
			}
			for i := 0; i < len(values) && i < len(sm.Headers); i++ {
				cellVal := values[i]
				colName, _ := excelize.ColumnNumberToName(i + 1)
				cellRef := fmt.Sprintf("%s%d", colName, rowIdx)
				cellType, _ := fTemplate.GetCellType(sheet, cellRef)
				if cellType != excelize.CellTypeNumber {
					continue
				}
				decimals := 0
				if parts := strings.Split(cellVal, "."); len(parts) == 2 {
					decimals = len(parts[1])
				}
				styleID, err := fTemplate.GetCellStyle(sheet, cellRef)
				if err != nil {
					continue
				}
				cacheKey := fmt.Sprintf("%d_%d", i, decimals)
				if _, ok := sm.StyleCache[cacheKey]; !ok {
					sm.StyleCache[cacheKey] = styleID
				}
			}
			rowIdx++
		}
	}

	return nil
}

func (sm *StreamMerger) MergeFiles(cfg *config.Config) ([]string, error) {

	sm.Cfg = cfg
	sm.PartCounter = 1

	// Удаляем старые файлы перед началом
	if err := removeExistingPartFiles(cfg); err != nil {
		return nil, err
	}

	// получаем список входящих файлов и путь к файлу шаблона
	inputFiles, templatePath, err := getInputFilesAndTemplatePath(cfg)
	if err != nil {
		return nil, err
	}

	sm.Cfg.TemplatePath = templatePath

	sm.UseTemplate = cfg.TemplatePath != ""

	// подготовки заголовков, стилей и типов данных из шаблона
	if err := sm.prepareTemplate(); err != nil {
		return nil, err
	}

	// инициализация StreamWriter
	if err := sm.newOutput(); err != nil {
		return nil, err
	}

	// Обход всех файлов
	for _, path := range inputFiles {
		if err := sm.processInputFile(path); err != nil {
			return nil, err
		}
	}

	// Заключительный flush
	if err := sm.StreamWriter.Flush(); err != nil {
		return nil, fmt.Errorf("ошибка финального flush: %v", err)
	}
	fileName := fmt.Sprintf("%s_part%d.xlsx", strings.TrimSuffix(cfg.OutputPath, ".xlsx"), sm.PartCounter)
	if err := sm.OutFile.SaveAs(fileName); err != nil {
		return nil, fmt.Errorf("ошибка сохранения файла: %v", err)
	}

	sm.OutputFiles = append(sm.OutputFiles, fileName)

	_ = sm.OutFile.Close()

	return sm.OutputFiles, nil

}

func removeExistingPartFiles(cfg *config.Config) error {
	pattern := fmt.Sprintf("%s_part*.xlsx", strings.TrimSuffix(cfg.OutputPath, ".xlsx"))
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("ошибка поиска файлов по шаблону: %v", err)
	}

	for _, file := range files {
		if err := os.Remove(file); err != nil {
			return fmt.Errorf("ошибка удаления файла %s: %v", file, err)
		}
	}
	return nil
}

func isDateFormat(fmtID int) bool {
	switch fmtID {
	case 14, 15, 16, 17, 22, 27, 30, 36, 45, 46, 47:
		return true
	}
	return false
}

func isNumericFormat(fmtID int) bool {
	switch fmtID {
	case 1, 2, 3, 4, 10, 37, 38, 39, 40:
		return true
	}
	return false
}

func getInputFilesAndTemplatePath(cfg *config.Config) ([]string, string, error) {
	var (
		templatePath string
		maxFileSize  int64
	)

	inputFiles := []string{}

	err := filepath.Walk(cfg.InputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(path) != ".xlsx" {
			return nil
		}

		inputFiles = append(inputFiles, path)

		if cfg.TemplatePath == "" && info.Size() > maxFileSize {
			maxFileSize = info.Size()
			templatePath = path
		}

		return nil
	})
	if err != nil {
		return nil, "", fmt.Errorf("ошибка при обходе папки: %w", err)
	}

	if cfg.TemplatePath != "" {
		if _, err := os.Stat(cfg.TemplatePath); os.IsNotExist(err) {
			return nil, "", fmt.Errorf("шаблонный файл не найден: %s", cfg.TemplatePath)
		}
		templatePath = cfg.TemplatePath
	}

	if templatePath == "" {
		return nil, "", fmt.Errorf("не удалось определить шаблонный файл")
	}

	return inputFiles, templatePath, nil
}
