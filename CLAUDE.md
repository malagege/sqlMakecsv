# sqlMakecsv 專案說明

跨平台 Go CLI 工具：讀取 `sql/*.sql` 對資料庫執行查詢，輸出 CSV / XLSX。

## 常用指令

- 建置：`go build -o sqlMakecsv.exe .`
- 檢查：`go vet ./...`
- 端對端測試（不需外部資料庫）：在測試目錄放 `.env`（`DRIVER=sqlite`、`DATASOURCE=test.db`）與 `sql/*.sql` 後直接執行
- 交叉編譯：`CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build .`（所有預設驅動皆純 Go，不需 CGO）

## 架構

- [main.go](main.go)：全部主邏輯。`loadConfig()` 讀 .env → `run()` 連線並走訪 sql 檔 → `processFile()` 處理單一檔案（略過判斷、備份、輸出）
- [drivers.go](drivers.go)：預設驅動註冊（mysql / postgres / sqlserver / sqlite，皆純 Go）
- [drivers_odbc.go](drivers_odbc.go)：ODBC 驅動，build tag `windows || odbc`

## 注意事項

- 輸出檔名刻意保留 `.sql` 字尾（如 `users.sql.csv`），`MAKE_MODIFY` / `MAKE_NOFILE` 的比對邏輯依賴這個命名，不要改
- 連線字串（`DATASOURCE`）不可套用 `getenvClean()`，因為密碼可能含 `#`
- 相容舊設定：`DATASOCURE`（拼字錯誤）與 `sqlite3` 驅動名稱都要繼續支援
- SQLite 用 modernc.org/sqlite（純 Go），不是 mattn/go-sqlite3（需 CGO）
- 每次完成修改後：`go build` + `go vet` 通過、用 SQLite 跑一次端對端驗證，再 git commit
