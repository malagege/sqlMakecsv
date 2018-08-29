package main

import (
	"database/sql"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/joho/sqltocsv"
)

// for log
var (
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
)

func init() {
	infoFile, err := os.OpenFile("info.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	errFile, err := os.OpenFile("error.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalln("打開日誌文件失敗：", err)
	}

	Info = log.New(io.MultiWriter(os.Stdout, infoFile), "Info:", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(io.MultiWriter(os.Stderr, errFile), "Error:", log.Ldate|log.Ltime|log.Lshortfile)

}

func main() {
	_ = os.Mkdir("sql", 744)
	_ = os.Mkdir("csv", 744)
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

	Info.Println("正在讀取路徑")

	sqlfiles, err := filepath.Glob("./sql/*.sql")

	Info.Println("讀取路徑完成")

	if err != nil {
		Error.Println("讀取SQL路徑有問題")
		Error.Panic(err)
	}

	Info.Println("正在DB連線")

	db, err := sql.Open(driver, datasocure)

	if err != nil {
		Error.Println("DB連線失敗")
		Error.Panic(err)
	}

	for i := 0; i < len(sqlfiles); i++ {
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
			Error.Println("SQL查詢錯誤")
			Error.Println(sqlstr)
		}

		csvConverterf := sqltocsv.New(rows)

		csvConverterf.WriteHeaders = writeheader
		Info.Println("產生csv中...")
		err = csvConverterf.WriteFile("./csv/" + filepath.Base(sqlfiles[i]) + ".csv")
		if err != nil {
			Error.Println("產生csv發生ERROR")
			Error.Println(err)
		} else {
			Info.Println("產生csv完成")
		}

	}

	defer db.Close()
	Info.Println("sqlMakecsv執行完畢")
}
