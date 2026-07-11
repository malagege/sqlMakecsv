// sqlMakecsv：讀取 sql/ 目錄下的 .sql 檔，逐一對資料庫執行查詢，
// 並將結果輸出成 csv 或 xlsx 檔案。設定由 .env（或環境變數）提供。
package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/joho/godotenv"
	"github.com/joho/sqltocsv"
	"github.com/malagege/sql2xlsx"
)

// 全域 logger，依 DISPLAY_MODE 決定輸出到畫面與檔案的組合
var (
	Info  *log.Logger
	Error *log.Logger
	Debug *log.Logger
)

// config 集中管理所有設定值
type config struct {
	driver      string // 資料庫驅動：mysql / postgres / sqlserver / sqlite / odbc
	dataSource  string // 連線字串
	writeHeader bool   // 是否輸出欄位名稱列
	makeMode    string // MAKE_ALL / MAKE_MODIFY / MAKE_NOFILE
	backupFile  bool   // 產生前是否把舊檔搬到 bak/
	fileType    string // csv / xlsx
}

// getenvClean 讀取環境變數並去除行內 # 註解與頭尾空白。
// 只用在「選項型」設定；連線字串不可用（密碼可能含 #）。
func getenvClean(key string) string {
	v := os.Getenv(key)
	if i := strings.Index(v, "#"); i >= 0 {
		v = v[:i]
	}
	return strings.TrimSpace(v)
}

func loadConfig() config {
	cfg := config{
		driver:      strings.TrimSpace(os.Getenv("DRIVER")),
		dataSource:  strings.TrimSpace(os.Getenv("DATASOURCE")),
		writeHeader: strings.EqualFold(getenvClean("WRITEHEADER"), "true"),
		makeMode:    strings.ToUpper(getenvClean("MAKE_MODE")),
		backupFile:  strings.EqualFold(getenvClean("BACKUP_FILE"), "true"),
		fileType:    strings.ToLower(getenvClean("FILE_TYPE")),
	}
	// 相容舊版設定檔的 DATASOCURE 拼字
	if cfg.dataSource == "" {
		cfg.dataSource = strings.TrimSpace(os.Getenv("DATASOCURE"))
	}
	// 舊版 sqlite3 驅動改用純 Go 的 modernc.org/sqlite，驅動名稱是 sqlite
	if cfg.driver == "sqlite3" {
		cfg.driver = "sqlite"
	}
	if cfg.fileType != "xlsx" {
		cfg.fileType = "csv"
	}
	return cfg
}

// setupLoggers 依 DISPLAY_MODE 建立 Info / Error / Debug logger。
// 回傳關閉 log 檔的函式。
func setupLoggers(displayMode string) (func(), error) {
	infoFile, err := os.OpenFile("info.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("打開 info.log 失敗: %w", err)
	}
	errFile, err := os.OpenFile("error.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		infoFile.Close()
		return nil, fmt.Errorf("打開 error.log 失敗: %w", err)
	}

	flags := log.Ldate | log.Ltime | log.Lshortfile
	var infoW, errW, debugW io.Writer
	switch displayMode {
	case "SHOW_INFO":
		infoW = io.MultiWriter(os.Stdout, infoFile)
		errW = io.MultiWriter(os.Stderr, infoFile, errFile)
		debugW = infoFile
	case "SHOW_ERROR":
		infoW = infoFile
		errW = io.MultiWriter(os.Stderr, infoFile, errFile)
		debugW = io.Discard
	case "HIDE_ALL":
		infoW = infoFile
		errW = io.MultiWriter(infoFile, errFile)
		debugW = io.Discard
	default: // SHOW_ALL
		infoW = io.MultiWriter(os.Stdout, infoFile)
		errW = io.MultiWriter(os.Stdout, infoFile, errFile)
		debugW = io.MultiWriter(os.Stdout, infoFile)
	}
	Info = log.New(infoW, "Info:", flags)
	Error = log.New(errW, "Error:", flags)
	Debug = log.New(debugW, "Debug:", flags)

	return func() {
		infoFile.Close()
		errFile.Close()
	}, nil
}

func main() {
	// .env 不存在時不視為錯誤（可改用系統環境變數），其他錯誤才中止
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Fatalln("載入 .env 設定檔出問題：", err)
	}

	closeLogs, err := setupLoggers(strings.ToUpper(getenvClean("DISPLAY_MODE")))
	if err != nil {
		log.Fatalln(err)
	}
	defer closeLogs()

	if err := run(loadConfig()); err != nil {
		Error.Println(err)
		os.Exit(1)
	}
}

func run(cfg config) error {
	Info.Println("sqlMakecsv 開始執行")

	if cfg.driver == "" || cfg.dataSource == "" {
		return fmt.Errorf("DRIVER 或 DATASOURCE 未設定，請確認 .env（可參考 .env.example）")
	}

	for _, dir := range []string{"sql", cfg.fileType, "bak"} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("建立目錄 %s 失敗: %w", dir, err)
		}
	}

	sqlFiles, err := filepath.Glob(filepath.Join("sql", "*.sql"))
	if err != nil {
		return fmt.Errorf("讀取 SQL 路徑有問題: %w", err)
	}
	if len(sqlFiles) == 0 {
		Info.Println("sql 目錄下沒有 .sql 檔案，沒有事情可做")
		return nil
	}

	Info.Println("正在連線資料庫（driver=" + cfg.driver + "）")
	db, err := sql.Open(cfg.driver, cfg.dataSource)
	if err != nil {
		return fmt.Errorf("DB 建立失敗: %w", err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		return fmt.Errorf("DB 連線失敗: %w", err)
	}

	failed := 0
	for _, sqlFile := range sqlFiles {
		if err := processFile(db, cfg, sqlFile); err != nil {
			Error.Println(sqlFile + " 處理失敗：" + err.Error())
			failed++
		}
	}

	Info.Println("sqlMakecsv 執行完畢")
	if failed > 0 {
		return fmt.Errorf("共 %d 個 SQL 檔處理失敗，詳見 error.log", failed)
	}
	return nil
}

// processFile 執行單一 SQL 檔並輸出結果檔
func processFile(db *sql.DB, cfg config, sqlFile string) error {
	outPath := filepath.Join(cfg.fileType, filepath.Base(sqlFile)+"."+cfg.fileType)
	outStat, outErr := os.Stat(outPath)
	outExists := outErr == nil

	// 依 MAKE_MODE 決定是否略過
	switch cfg.makeMode {
	case "MAKE_MODIFY":
		if outExists {
			if sqlStat, err := os.Stat(sqlFile); err == nil && sqlStat.ModTime().Before(outStat.ModTime()) {
				Info.Println(sqlFile + " 沒有比 " + outPath + " 新，不做產生動作")
				return nil
			}
		}
	case "MAKE_NOFILE":
		if outExists {
			Info.Println(sqlFile + " 已經有 " + outPath + "，不做產生動作")
			return nil
		}
	}

	Info.Println("正在讀取 " + sqlFile)
	sqlBytes, err := os.ReadFile(sqlFile)
	if err != nil {
		return fmt.Errorf("讀取 SQL 檔失敗: %w", err)
	}
	sqlStr := string(sqlBytes)

	Info.Println("正在執行 SQL：" + sqlStr)
	rows, err := db.Query(sqlStr)
	if err != nil {
		return fmt.Errorf("SQL 查詢錯誤: %w", err)
	}
	defer rows.Close()

	// 產生前先備份舊檔到 bak/（檔名帶上舊檔的修改時間）
	if cfg.backupFile && outExists {
		stamp := outStat.ModTime().Format("20060102_150405")
		bakPath := filepath.Join("bak", filepath.Base(sqlFile)+"_"+stamp+"."+cfg.fileType)
		if err := os.Rename(outPath, bakPath); err != nil {
			Error.Println(outPath + " 備份失敗：" + err.Error())
		} else {
			Info.Println(outPath + " 順利備份到 " + bakPath)
		}
	}

	Info.Println("產生 " + outPath + " 中...")
	if cfg.fileType == "xlsx" {
		err = sql2xlsx.GenerateXLSXFromRows(rows, outPath, cfg.writeHeader)
	} else {
		converter := sqltocsv.New(rows)
		converter.WriteHeaders = cfg.writeHeader
		err = converter.WriteFile(outPath)
	}
	if err != nil {
		return fmt.Errorf("產生 %s 發生錯誤: %w", cfg.fileType, err)
	}
	Info.Println("產生 " + outPath + " 完成")
	return nil
}
