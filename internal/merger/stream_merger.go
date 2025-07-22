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
	BufferSize int
}

func NewStreamMerger() FileMerger {
	sm := &StreamMerger{
		BufferSize: 1000,
	}
	sm.BaseMerger.Init() // Инициализация базовой части
	return sm
}

func (sm *StreamMerger) MergeFiles(cfg *config.Config) ([]string, error) {

	var (
		templatePath string
		maxFileSize  int64
	)

	inputFiles := []string{}
	_ = filepath.Walk(cfg.InputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || filepath.Ext(path) != ".xlsx" {
			return nil
		}

		if info.Size() > maxFileSize {
			maxFileSize = info.Size()
			templatePath = path
		}

		inputFiles = append(inputFiles, path)

		return nil
	})

	if templatePath == "" {
		return nil, fmt.Errorf("нет файлов для объединения")
	}

	partCounter := 1
	rowCounter := int64(0)
	var outFile *excelize.File
	var streamWriter *excelize.StreamWriter
	var sheet string
	outputFiles := []string{}

	newOutput := func() error {
		if outFile != nil {
			if err := streamWriter.Flush(); err != nil {
				return fmt.Errorf("ошибка финального flush: %v", err)
			}
			fileName := fmt.Sprintf("%s_part%d.xlsx", strings.TrimSuffix(cfg.OutputPath, ".xlsx"), partCounter)
			_ = os.Remove(fileName)
			if err := outFile.SaveAs(fileName); err != nil {
				return fmt.Errorf("ошибка сохранения файла: %v", err)
			}
			_ = outFile.Close()
			outputFiles = append(outputFiles, fileName)
			partCounter++
		}

		var err error
		outFile, err = excelize.OpenFile(templatePath)
		if err != nil {
			return fmt.Errorf("ошибка открытия шаблона: %v", err)
		}
		sheetList := outFile.GetSheetList()
		if len(sheetList) == 0 {
			return fmt.Errorf("шаблон пустой, нет листов")
		}
		sheet = "merged"
		outFile.NewSheet(sheet)

		// 2. Копируем ширину для каждой колонки
		for colIdx := 1; colIdx <= len(sm.Headers); colIdx++ {
			// Получаем имя колонки (например, "A", "B")
			colName, _ := excelize.ColumnNumberToName(colIdx)

			// Получаем ширину колонки из исходного листа
			width, err := outFile.GetColWidth(sheetList[0], colName)
			if err != nil {
				continue // Пропускаем ошибки
			}

			// Устанавливаем такую же ширину в целевом листе
			outFile.SetColWidth(sheet, colName, colName, width)
		}

		streamWriter, err = outFile.NewStreamWriter(sheet)
		if err != nil {
			return fmt.Errorf("ошибка создания StreamWriter: %v", err)
		}

		outFile.DeleteSheet(sheetList[0])
		rowCounter = 0
		return nil
	}

	outFile, err := excelize.OpenFile(templatePath)
	if err != nil {
		return nil, fmt.Errorf("ошибка открытия шаблона: %v", err)
	}
	sheetList := outFile.GetSheetList()
	if len(sheetList) == 0 {
		return nil, fmt.Errorf("шаблон пустой, нет листов")
	}
	sheet = sheetList[0]

	rows, err := outFile.Rows(sheet)
	if err != nil {
		return nil, err
	}

	// Получаем заголовки из первого файла
	if len(sm.Headers) == 0 && rows.Next() {
		headers, _ := rows.Columns()
		if cfg.AddSourceFile {
			if cfg.HasHeaders {
				sm.Headers = append(headers, "SourceFile")
			} else {
				sm.Headers = append(headers, "")
			}
		} else {
			sm.Headers = headers
		}
	}

	// Получение высоты для строки заголовка
	heightHeader, _ := outFile.GetRowHeight(sheet, 1)

	// Получаем стили заголовков (первой строки) и первой строки данных (второй строки)
	headerStyles := make([]int, len(sm.Headers))
	rowStyles := make([]int, len(sm.Headers))
	for col := 1; col <= len(sm.Headers); col++ {
		cell1, _ := excelize.CoordinatesToCellName(col, 1)
		styleID1, err := outFile.GetCellStyle(sheet, cell1)
		if err != nil {
			styleID1 = 0
		}
		headerStyles[col-1] = styleID1

		cell2, _ := excelize.CoordinatesToCellName(col, 2)
		styleID2, err := outFile.GetCellStyle(sheet, cell2)
		if err != nil {
			styleID2 = 0
		}
		rowStyles[col-1] = styleID2
	}

	// заполняем кеш стилей
	styleCache := make(map[string]int) // cacheKey = colIndex_decimals

	tmplRows, err := outFile.Rows(sheet)
	if err != nil {
		return nil, fmt.Errorf("ошибка чтения строк шаблона: %v", err)
	}

	rowIdx := 1
	for tmplRows.Next() && rowIdx <= cfg.SampleRows {
		values, err := tmplRows.Columns()
		if err != nil {
			continue
		}

		for i := 0; i < len(values) && i < len(sm.Headers); i++ {
			cellVal := values[i]
			colName, _ := excelize.ColumnNumberToName(i + 1)
			cellRef := fmt.Sprintf("%s%d", colName, rowIdx)

			cellType, _ := outFile.GetCellType(sheet, cellRef)
			if cellType != excelize.CellTypeNumber {
				continue
			}

			decimals := 0
			if parts := strings.Split(cellVal, "."); len(parts) == 2 {
				decimals = len(parts[1])
			}

			styleID, err := outFile.GetCellStyle(sheet, cellRef)
			if err != nil {
				continue
			}

			cacheKey := fmt.Sprintf("%d_%d", i, decimals)
			if _, ok := styleCache[cacheKey]; !ok {
				styleCache[cacheKey] = styleID
			}

		}
		rowIdx++
	}

	_ = outFile.Close()
	outFile = nil

	if err := newOutput(); err != nil {
		return nil, err
	}

	// Обход всех файлов
	for _, path := range inputFiles {

		f, err := excelize.OpenFile(path)
		if err != nil {
			return nil, fmt.Errorf("ошибка открытия файла %s: %v", path, err)
		}

		sheetListSrc := f.GetSheetList()
		if len(sheetListSrc) == 0 {
			continue
		}
		sheetSrc := sheetListSrc[0]
		rows, err := f.Rows(sheetSrc)
		if err != nil {
			return nil, fmt.Errorf("ошибка чтения строк из %s: %v", path, err)
		}

		rowInFile := 1

		if cfg.HasHeaders {
			rows.Next() // пропуск первой строки
			rowInFile++
		}

		for rows.Next() {

			if cfg.MaxRowPerFile > 0 && rowCounter >= cfg.MaxRowPerFile {
				if err := newOutput(); err != nil {
					return nil, err
				}
			}

			if rowCounter == 0 {
				// Пишем заголовки при каждом новом файле
				// Записываем заголовки с применением стилей
				if cfg.HasHeaders && len(sm.Headers) > 0 {
					rowCounter = 1
					headerRow := make([]interface{}, len(sm.Headers))
					for i, h := range sm.Headers {
						headerRow[i] = excelize.Cell{
							Value:   h,
							StyleID: headerStyles[i],
						}
					}

					cell := fmt.Sprintf("A%d", rowCounter)
					if err := streamWriter.SetRow(cell, headerRow, excelize.RowOpts{Height: heightHeader}); err != nil {
						return nil, fmt.Errorf("ошибка записи заголовков: %v", err)
					}
					rowCounter++
				}
			}

			stringRow, err := rows.Columns()
			if err != nil {
				return nil, fmt.Errorf("ошибка чтения строки: %v", err)
			}

			rowData := make([]interface{}, len(stringRow))

			for i, cellVal := range stringRow {

				styleID := 0
				if i < len(rowStyles) {
					styleID = rowStyles[i]
				}
				rowData[i] = excelize.Cell{
					Value:   cellVal,
					StyleID: styleID,
				}
				colName, _ := excelize.ColumnNumberToName(i + 1)
				cellRef := fmt.Sprintf("%s%d", colName, rowInFile)

				valType, _ := f.GetCellType(sheetSrc, cellRef)

				var value interface{}
				switch valType {
				case excelize.CellTypeBool:
					value = (cellVal == "1" || strings.ToLower(cellVal) == "true")
				case excelize.CellTypeNumber:
					if n, err := strconv.ParseFloat(cellVal, 64); err == nil {

						value = n

						// Определение количества знаков после запятой
						decimals := 0
						if parts := strings.Split(cellVal, "."); len(parts) == 2 {
							decimals = len(parts[1])
						}

						cacheKey := fmt.Sprintf("%d_%d", i, decimals)
						if cachedStyle, ok := styleCache[cacheKey]; ok {
							styleID = cachedStyle
						} else {
							styleCache[cacheKey] = styleID // сохранить базовый как fallback
						}
					} else {
						value = cellVal
					}
				case excelize.CellTypeDate:
					value = cellVal // Можно попробовать конвертировать, но Excel форматирует их как строки
				default:
					value = cellVal
				}

				rowData[i] = excelize.Cell{
					Value:   value,
					StyleID: styleID,
				}
			}

			if cfg.AddSourceFile {
				rowData = append(rowData, filepath.Base(path))
			}

			// Получение высоты для строки rowInFile
			height, _ := f.GetRowHeight(sheetSrc, rowInFile)

			cell := fmt.Sprintf("A%d", rowCounter)
			if err := streamWriter.SetRow(cell, rowData, excelize.RowOpts{Height: height}); err != nil {
				return nil, fmt.Errorf("ошибка записи строки: %v", err)
			}

			rowCounter++
			rowInFile++
		}

		_ = f.Close()

	}

	// Заключительный flush
	if err := streamWriter.Flush(); err != nil {
		return nil, fmt.Errorf("ошибка финального flush: %v", err)
	}
	fileName := fmt.Sprintf("%s_part%d.xlsx", strings.TrimSuffix(cfg.OutputPath, ".xlsx"), partCounter)
	_ = os.Remove(fileName)
	if err := outFile.SaveAs(fileName); err != nil {
		return nil, fmt.Errorf("ошибка сохранения файла: %v", err)
	}
	outputFiles = append(outputFiles, fileName)

	_ = outFile.Close()

	return outputFiles, nil

}
