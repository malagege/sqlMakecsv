# sqlMakecsv

跨平台的 SQL 批次匯出工具：讀取 `sql/` 目錄下的 `.sql` 檔，逐一對資料庫執行查詢，把結果輸出成 **CSV** 或 **XLSX** 檔案。

支援 MySQL、PostgreSQL、SQL Server、SQLite、ODBC（Windows）。

## 下載

[Releases](https://github.com/malagege/sqlMakecsv/releases) 頁面提供各平台的現成執行檔：

| 平台 | 檔案 |
|------|------|
| Windows x64 | `sqlMakecsv-<版本>-windows-amd64.zip` |
| Windows ARM64 | `sqlMakecsv-<版本>-windows-arm64.zip`（不含 ODBC 驅動） |
| Linux x64 | `sqlMakecsv-<版本>-linux-amd64.tar.gz` |
| Linux ARM64（樹莓派 3/4/5 的 64 位元系統） | `sqlMakecsv-<版本>-linux-arm64.tar.gz` |
| Linux ARMv6（樹莓派 32 位元系統，含 Pi 1 / Zero） | `sqlMakecsv-<版本>-linux-armv6.tar.gz` |
| macOS Intel | `sqlMakecsv-<版本>-darwin-amd64.tar.gz` |
| macOS Apple Silicon | `sqlMakecsv-<版本>-darwin-arm64.tar.gz` |

發布新版本：推送 `v` 開頭的 tag（例如 `git tag v1.0.0 && git push origin v1.0.0`），
GitHub Actions 會自動建置以上所有平台並建立 Release。

## 快速開始

```
# 1. 建置（需要 Go 1.24+）
go build -o sqlMakecsv.exe .

# 2. 建立設定檔
copy .env.example .env
（依你的資料庫修改 DRIVER 和 DATASOURCE）

# 3. 把要執行的 SQL 放進 sql/ 目錄
echo SELECT * FROM users > sql\users.sql

# 4. 執行
sqlMakecsv.exe
```

執行後結果會輸出到 `csv/users.sql.csv`（或 `xlsx/users.sql.xlsx`）。

## 設定說明（.env）

| 設定 | 說明 | 值 |
|------|------|-----|
| `DRIVER` | 資料庫驅動 | `mysql` / `postgres` / `sqlserver` / `sqlite` / `odbc` |
| `DATASOURCE` | 連線字串（見下方範例） | |
| `WRITEHEADER` | 是否輸出欄位名稱列 | `true` / `false` |
| `DISPLAY_MODE` | 畫面顯示哪些訊息（log 檔一律都寫） | `SHOW_ALL` / `SHOW_INFO` / `SHOW_ERROR` / `HIDE_ALL` |
| `MAKE_MODE` | 產生模式 | `MAKE_ALL`（每次重做）/ `MAKE_MODIFY`（SQL 較新才重做）/ `MAKE_NOFILE`（無輸出檔才做） |
| `BACKUP_FILE` | 重新產生前把舊檔備份到 `bak/` | `true` / `false` |
| `FILE_TYPE` | 輸出格式 | `csv` / `xlsx` |

`.env` 不存在時會直接讀取系統環境變數，方便排程或容器使用。
（相容舊版：`DATASOCURE` 拼字與 `sqlite3` 驅動名稱仍可使用。）

### 連線字串範例

```
# MySQL
DRIVER=mysql
DATASOURCE=user:password@tcp(localhost:3306)/dbname?timeout=15s

# PostgreSQL
DRIVER=postgres
DATASOURCE=postgres://user:password@localhost:5432/dbname?sslmode=disable

# SQL Server
DRIVER=sqlserver
DATASOURCE=sqlserver://user:password@localhost:1433?database=dbname

# SQLite（純 Go 實作，不需要安裝任何東西）
DRIVER=sqlite
DATASOURCE=data.db

# ODBC（Windows 預設內建）
DRIVER=odbc
DATASOURCE=DSN=mydsn;UID=user;PWD=password
```

## 跨平台建置

所有預設驅動都是純 Go 實作，**不需要 CGO / gcc**，在任何平台都能直接交叉編譯：

```
# Windows 上編出 Linux / macOS 執行檔
set CGO_ENABLED=0
set GOOS=linux
set GOARCH=amd64
go build -o dist/sqlMakecsv-linux-amd64 .

set GOOS=darwin
set GOARCH=arm64
go build -o dist/sqlMakecsv-darwin-arm64 .
```

ODBC 驅動在 Windows x86/x64 預設內建（走系統呼叫，不需 CGO）；
windows/arm64 因上游套件不支援而不含 ODBC。
Linux / macOS 如需 ODBC，須安裝 unixODBC 後用 `go build -tags odbc .` 建置。

## 目錄結構

```
sql/    放要執行的 .sql 檔（一檔一個查詢）
csv/    CSV 輸出結果
xlsx/   XLSX 輸出結果
bak/    舊輸出檔的備份（BACKUP_FILE=true 時）
*.log   info.log 記錄所有訊息、error.log 只記錄錯誤
```
