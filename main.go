package main

import (
	"database/sql"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/alexbrainman/odbc"
	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"

	"github.com/joho/godotenv"
	"github.com/joho/sqltocsv"
	"github.com/malagege/sql2xlsx"
)

// for log
var (
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
	Debug   *log.Logger
)

func init() {
	err := godotenv.Load()
	if err != nil {
		Error.Println("載入設定檔出問題")
		Error.Println(err)
		os.Exit(1)
	}
	infoFile, err := os.OpenFile("info.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	errFile, err := os.OpenFile("error.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalln("打開日誌文件失敗：", err)
	}
	switch strings.ToUpper(os.Getenv("DISPLAY_MODE")) {
	case "SHOW_INFO":
		Info = log.New(io.MultiWriter(os.Stdout, infoFile), "Info:", log.Ldate|log.Ltime|log.Lshortfile)
		Error = log.New(io.MultiWriter(os.Stderr, infoFile, errFile), "Error:", log.Ldate|log.Ltime|log.Lshortfile)
		Debug = log.New(io.MultiWriter(infoFile), "Debug:", log.Ldate|log.Ltime|log.Lshortfile)
		break
	case "SHOW_ERROR":
		Info = log.New(io.MultiWriter(infoFile), "Info:", log.Ldate|log.Ltime|log.Lshortfile)
		Error = log.New(io.MultiWriter(os.Stderr, infoFile, errFile), "Error:", log.Ldate|log.Ltime|log.Lshortfile)
		Debug = log.New(io.MultiWriter(ioutil.Discard), "Debug:", log.Ldate|log.Ltime|log.Lshortfile)
		break
	case "HIDE_ALL":
		Info = log.New(io.MultiWriter(infoFile), "Info:", log.Ldate|log.Ltime|log.Lshortfile)
		Error = log.New(io.MultiWriter(infoFile, errFile), "Error:", log.Ldate|log.Ltime|log.Lshortfile)
		Debug = log.New(io.MultiWriter(ioutil.Discard), "Debug:", log.Ldate|log.Ltime|log.Lshortfile)
		break
	default: //SHOW_ALL
		Info = log.New(io.MultiWriter(os.Stdout, infoFile), "Info:", log.Ldate|log.Ltime|log.Lshortfile)
		Error = log.New(io.MultiWriter(os.Stdout, infoFile, errFile), "Error:", log.Ldate|log.Ltime|log.Lshortfile)
		Debug = log.New(io.MultiWriter(os.Stdout, infoFile), "Debug:", log.Ldate|log.Ltime|log.Lshortfile)
	}
}

func main() {
	_ = os.Mkdir("sql", 0755)
	_ = os.Mkdir("csv", 0755)
	_ = os.Mkdir("xlsx", 0755)
	_ = os.Mkdir("bak", 0755)
	Info.Println("sqlMakecsv開始執行")
	err := godotenv.Load()
	if err != nil {
		Error.Println("載入設定檔出問題")
		Error.Println(err)
		os.Exit(1)
	}
	driver := os.Getenv("DRIVER")
	datasocure := os.Getenv("DATASOCURE")
	var writeheader bool
	if strings.ToLower(os.Getenv("WRITEHEADER")) == "true" {
		writeheader = true
	} else {
		writeheader = false
	}

	var file_type string
	if strings.ToLower(os.Getenv("FILE_TYPE")) == "xlsx" {
		file_type = "xlsx"
	} else {
		file_type = "csv"
	}

	Info.Println("正在讀取路徑")

	sqlfiles, err := filepath.Glob("./sql/*.sql")
	csvfiles, err := filepath.Glob("./" + file_type + "/*." + file_type)
	//https://hsinyu.gitbooks.io/golang_note/content/map_1.html
	//
	csvfilesMap := map[string]int64{}
	for i := range csvfiles {
		fi, err := os.Stat(csvfiles[i])
		if err != nil {
			Error.Println(csvfiles[i] + "無法得到檔案狀況")
		}
		m1 := fi.ModTime()
		csvfilesMap[csvfiles[i]] = m1.Unix()
	}
	Info.Println("讀取路徑完成")

	if err != nil {
		Error.Println("讀取SQL路徑有問題")
		Error.Panic(err)
	}

	Info.Println("正在DB連線")

	db, err := sql.Open(driver, datasocure)

	if err != nil {
		Error.Println("DB建立失敗")
		Error.Panic(err)
	}

	err = db.Ping()

	if err != nil {
		Error.Println("DB連線失敗")
		Error.Panic(err)
	}

	for i := 0; i < len(sqlfiles); i++ {

		//檢查是否要讀取資料
		switch strings.ToUpper(os.Getenv("MAKE_MODE")) {
		case "MAKE_ALL":
			break
		case "MAKE_MODIFY":
			if val, ok := csvfilesMap[file_type+string(os.PathSeparator)+filepath.Base(sqlfiles[i])+"."+file_type]; ok {
				if ff, _ := os.Stat(sqlfiles[i]); ff.ModTime().Unix() < val {
					Info.Println(sqlfiles[i] + "更新時間大於" + file_type + "，不做產生動作")
					continue
				}
			}
			break
		case "MAKE_NOFILE":
			if _, ok := csvfilesMap[file_type+string(os.PathSeparator)+filepath.Base(sqlfiles[i])+"."+file_type]; ok {
				Info.Println(sqlfiles[i] + "已經有" + file_type + "，不做產生動作")
				continue
			}
			break
		}

		Info.Println("正在讀取" + sqlfiles[i])
		sqls, err := ioutil.ReadFile(sqlfiles[i])
		if err != nil {
			Error.Println("讀取" + sqlfiles[i] + "發生錯誤，SQL如下")
			Error.Println(sqls)
			Error.Println(err)
			Error.Panic(err)
		}
		sqlstr := string(sqls)

		Info.Println("讀取到SQL:" + sqlstr)
		Info.Println("正在執行")
		rows, err := db.Query(sqlstr)

		if err != nil {
			Error.Println("SQL查詢錯誤:")
			Error.Println(err)
			Error.Println(sqlstr)
			continue
		}

		//備份csv
		if strings.ToLower(os.Getenv("BACKUP_FILE")) == "true" {
			Debug.Println(sqlfiles[i] + "備份檔案開始")

			isBak := true
			//檢查是否有檔案
			if _, ok := csvfilesMap[file_type+string(os.PathSeparator)+filepath.Base(sqlfiles[i])+"."+file_type]; !ok {
				Debug.Println(sqlfiles[i] + "沒有檔案，不做備份")
				isBak = false
			}
			if isBak {
				// t := time.Now().Local()
				ff, _ := os.Stat(file_type + string(os.PathSeparator) + filepath.Base(sqlfiles[i]) + "." + file_type)
				t := time.Unix(ff.ModTime().Unix(), 0)
				s := t.Format("20060102_150405")
				err = os.Rename(file_type+string(os.PathSeparator)+filepath.Base(sqlfiles[i])+"."+file_type, "bak/"+filepath.Base(sqlfiles[i])+"_"+s+"."+file_type)
				if err != nil {
					Error.Println(file_type + string(os.PathSeparator) + filepath.Base(sqlfiles[i]) + "." + file_type + "備份" + file_type + "檔案發生錯誤")
					Error.Println(err)
				} else {
					Info.Println(file_type + string(os.PathSeparator) + filepath.Base(sqlfiles[i]) + "順利備份完畢")
				}
			}
		}

		csvConverterf := sqltocsv.New(rows)
		csvConverterf.WriteHeaders = writeheader
		Info.Println("產生" + file_type + "中...")
		if file_type == "xlsx" {
			err = sql2xlsx.GenerateXLSXFromRows(rows, "./xlsx/"+filepath.Base(sqlfiles[i])+".xlsx", writeheader)
		} else {
			err = csvConverterf.WriteFile("./csv/" + filepath.Base(sqlfiles[i]) + ".csv")
		}
		if err != nil {
			Error.Println("產生" + file_type + "發生ERROR")
			Error.Println(err)
		} else {
			Info.Println("產生" + file_type + "完成")
		}

	}

	defer db.Close()
	Info.Println("sqlMakecsv執行完畢")
}
