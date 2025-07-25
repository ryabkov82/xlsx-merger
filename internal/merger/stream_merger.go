// Package merger предоставляет функционал для слияния XLSX файлов
// с поддержкой потоковой обработки и шаблонов.
package merger

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/ryabkov82/xlsx-merger/internal/config"
	"github.com/xuri/excelize/v2"
)

// RowPayload содержит данные строки для обработки
// FileIndex - индекс исходного файла
// Cells - значения ячеек строки
// Height - высота строки
type RowPayload struct {
	FileIndex int
	Cells     []interface{}
	Height    float64
	//Done      bool
}

// FileJob описывает задачу обработки файла
// Index - порядковый индекс файла
// Path - путь к файлу
type FileJob struct {
	Index int
	Path  string
}

// StreamMerger реализует потоковое слияние XLSX файлов
// Поддерживает:
// - обработку больших файлов с ограничением памяти
// - использование шаблонов для форматирования
// - разделение результата на части
type StreamMerger struct {
	BaseMerger // Встраиваем базовый функционал

	// Стили и форматирование
	RowStyles    []int               // Стили для строк данных
	HeaderStyles []int               // Стили для заголовков
	ValueTypes   []excelize.CellType // Типы данных для каждой колонки
	StyleCache   map[string]int      // Кеш стилей для числовых форматов

	// Конфигурация и состояние
	UseTemplate  bool                   // Флаг использования шаблона
	Cfg          *config.Config         // Конфигурация слияния
	StreamWriter *excelize.StreamWriter // Потоковый писатель Excel
	RowCounter   int64                  // Счетчик строк в текущем файле
	HeightHeader float64                // Высота строки заголовка
	Sheet        string                 // Имя листа для результатов
	OutFile      *excelize.File         // Текущий выходной файл
	PartCounter  int                    // Счетчик частей результата
	OutputFiles  []string               // Пути к созданным файлам
	RowCount     int64                  // Общее количество обработанных строк
}

// NewStreamMerger создает новый экземпляр StreamMerger
// Возвращает интерфейс FileMerger
func NewStreamMerger() FileMerger {
	sm := &StreamMerger{}
	sm.BaseMerger.Init() // Инициализация базовой части
	return sm
}

// newOutput создает новый выходной файл на основе шаблона
// Закрывает предыдущий файл если он был открыт
// Возвращает ошибку если:
// - не удалось создать файл
// - шаблон не содержит листов
// - не удалось создать StreamWriter
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

	// Создание нового файла на основе шаблона
	var err error
	sm.OutFile, err = excelize.OpenFile(sm.Cfg.TemplatePath)
	if err != nil {
		return fmt.Errorf("ошибка открытия шаблона: %v", err)
	}
	// Проверка наличия листов в шаблоне
	sheetList := sm.OutFile.GetSheetList()
	if len(sheetList) == 0 {
		return fmt.Errorf("шаблон пустой, нет листов")
	}
	// Настройка нового листа для результатов
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

	// Инициализация потокового писателя
	sm.StreamWriter, err = sm.OutFile.NewStreamWriter(sm.Sheet)
	if err != nil {
		return fmt.Errorf("ошибка создания StreamWriter: %v", err)
	}

	sm.OutFile.DeleteSheet(sheetList[0])
	sm.RowCounter = 0

	// Запись заголовков если требуется
	if sm.Cfg.HasHeaders && len(sm.Headers) > 0 {
		headerRow := make([]interface{}, len(sm.Headers))
		for i, h := range sm.Headers {
			headerRow[i] = excelize.Cell{
				Value:   h,
				StyleID: sm.HeaderStyles[i],
			}
		}

		cell := fmt.Sprintf("A%d", sm.RowCounter+1)
		if err := sm.StreamWriter.SetRow(cell, headerRow, excelize.RowOpts{Height: sm.HeightHeader}); err != nil {
			return fmt.Errorf("ошибка записи заголовка: %v", err)
		}
		sm.RowCounter++

	}

	return nil
}

// processInputFile читает файл, готовит rowData и отправляет в канал
func (sm *StreamMerger) processInputFile(ctx context.Context, fileIndex int, path string, rowChan chan<- RowPayload) error {

	defer close(rowChan)

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

		if ctx.Err() != nil {
			return ctx.Err()
		}
		rowChan <- RowPayload{
			FileIndex: fileIndex,
			Cells:     rowData,
			Height:    height,
		}

		rowInFile++
	}

	return nil
}

// prepareTemplate загружает и анализирует шаблон для:
// - определения структуры заголовков
// - извлечения стилей форматирования
// - определения типов данных
// Возвращает ошибку если:
// - шаблон не может быть открыт
// - шаблон не содержит данных
// - не удалось прочитать строки шаблона
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

// writerLoop читает из канала и пишет данные в streamWriter, переключая файлы по maxRow
func (sm *StreamMerger) writerLoop(ctx context.Context, cancel context.CancelFunc, rowChans []<-chan RowPayload, doneChan chan<- error) {

	for expected := 0; expected < len(rowChans); expected++ {
		ch := rowChans[expected]
		for {
			select {
			case <-ctx.Done():
				doneChan <- ctx.Err()
				return
			case payload, ok := <-ch:
				if !ok {
					// канал закрыт, переходим к следующему
					ch = nil
				} else {
					if sm.Cfg.MaxRowPerFile > 0 && sm.RowCounter >= sm.Cfg.MaxRowPerFile {
						if err := sm.newOutput(); err != nil {
							cancel() // посылаем сигнал записывающим горутинам
							doneChan <- fmt.Errorf("ошибка создания нового файла: %w", err)
							return
						}
					}
					cell := fmt.Sprintf("A%d", sm.RowCounter+1)
					if err := sm.StreamWriter.SetRow(cell, payload.Cells, excelize.RowOpts{Height: payload.Height}); err != nil {
						cancel()
						doneChan <- fmt.Errorf("ошибка записи строки: %w", err)
						return
					}
					sm.RowCounter++
					sm.RowCount++
				}
			}
			if ch == nil {
				break
			}
		}
	}

	if err := sm.StreamWriter.Flush(); err != nil {
		cancel()
		doneChan <- fmt.Errorf("ошибка финального flush: %w", err)
		return
	}
	fileName := fmt.Sprintf("%s_part%d.xlsx", strings.TrimSuffix(sm.Cfg.OutputPath, ".xlsx"), sm.PartCounter)
	if err := sm.OutFile.SaveAs(fileName); err != nil {
		cancel()
		doneChan <- fmt.Errorf("ошибка сохранения файла: %w", err)
		return
	}
	_ = sm.OutFile.Close()
	sm.OutputFiles = append(sm.OutputFiles, fileName)
	doneChan <- nil
}

// MergeFiles выполняет слияние файлов согласно конфигурации
// Потоково обрабатывает входные файлы с использованием worker-горутин
// Разделяет результат на части при превышении MaxRowPerFile
// Возвращает:
// - список созданных файлов
// - общее количество обработанных строк
// - ошибку если таковая возникла
func (sm *StreamMerger) MergeFiles(cfg *config.Config) ([]string, int64, error) {

	sm.Cfg = cfg
	sm.PartCounter = 1

	// Удаляем старые файлы перед началом
	if err := removeExistingPartFiles(cfg); err != nil {
		return nil, sm.RowCount, err
	}

	// получаем список входящих файлов и путь к файлу шаблона
	inputFiles, templatePath, err := getInputFilesAndTemplatePath(cfg)
	if err != nil {
		return nil, sm.RowCount, err
	}

	sm.Cfg.TemplatePath = templatePath

	sm.UseTemplate = cfg.TemplatePath != ""

	// подготовки заголовков, стилей и типов данных из шаблона
	if err := sm.prepareTemplate(); err != nil {
		return nil, sm.RowCount, err
	}

	// инициализация StreamWriter
	if err := sm.newOutput(); err != nil {
		return nil, sm.RowCount, err
	}

	workerCount := 4

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // гарантирует освобождение ресурсов

	// Создаем отдельный канал для каждого файла
	rowChans := make([]chan RowPayload, len(inputFiles))
	for i := range rowChans {
		rowChans[i] = make(chan RowPayload, 20000) // буфер на файл, можно менять
	}

	done := make(chan error)

	go sm.writerLoop(ctx, cancel, toReadOnlyChans(rowChans), done)

	// воркеры чтения
	var wg sync.WaitGroup
	fileCh := make(chan FileJob, workerCount)

	wg.Add(workerCount)

	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			for job := range fileCh {
				// Прекращаем работу, если контекст отменён
				if ctx.Err() != nil {
					return
				}

				if err := sm.processInputFile(ctx, job.Index, job.Path, rowChans[job.Index]); err != nil {
					// Отменяем контекст, чтобы остальные остановились
					cancel()
					// Ошибка может быть записана только один раз в канал done:
					select {
					case done <- err:
					default:
					}
					return
				}
			}
		}()
	}

	// Отправка путей
	go func() {
		for i, path := range inputFiles {
			fileCh <- FileJob{Index: i, Path: path}
		}
		close(fileCh)
	}()

	wg.Wait()

	err = <-done
	close(done)

	return sm.OutputFiles, sm.RowCount, err
}

// Вспомогательная функция для преобразования []chan T в []<-chan T
func toReadOnlyChans(chans []chan RowPayload) []<-chan RowPayload {
	ro := make([]<-chan RowPayload, len(chans))
	for i, ch := range chans {
		ro[i] = ch
	}
	return ro
}

// removeExistingPartFiles удаляет существующие частичные файлы результата
// Используется для очистки перед новым слиянием
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

// getInputFilesAndTemplatePath собирает входные файлы и определяет шаблон
// Возвращает:
// - список XLSX файлов в директории
// - путь к шаблону (наибольший файл или из конфига)
// - ошибку если файлы не найдены или шаблон недоступен
func getInputFilesAndTemplatePath(cfg *config.Config) ([]string, string, error) {
	type fileWithSize struct {
		Path string
		Size int64
	}

	var templatePath string

	entries, err := os.ReadDir(cfg.InputDir)
	if err != nil {
		return nil, "", fmt.Errorf("ошибка при чтении директории: %w", err)
	}

	var files []fileWithSize

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".xlsx" {
			continue
		}

		fullPath := filepath.Join(cfg.InputDir, entry.Name())
		info, err := entry.Info()
		if err != nil {
			continue
		}

		files = append(files, fileWithSize{
			Path: fullPath,
			Size: info.Size(),
		})
	}

	if len(files) == 0 {
		return nil, "", fmt.Errorf("не найдено .xlsx файлов в директории")
	}

	// Сортировка по размеру по возрастанию
	sort.Slice(files, func(i, j int) bool {
		return files[i].Size < files[j].Size
	})

	inputFiles := make([]string, len(files))
	for i, f := range files {
		inputFiles[i] = f.Path
	}

	// Определение шаблона
	if cfg.TemplatePath != "" {
		if _, err := os.Stat(cfg.TemplatePath); os.IsNotExist(err) {
			return nil, "", fmt.Errorf("шаблонный файл не найден: %s", cfg.TemplatePath)
		}
		templatePath = cfg.TemplatePath
	} else {
		templatePath = inputFiles[len(inputFiles)-1] // самый большой файл
	}

	return inputFiles, templatePath, nil
}
