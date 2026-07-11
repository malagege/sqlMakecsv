// 預設內建的資料庫驅動，全部為純 Go 實作，
// 因此可以用 CGO_ENABLED=0 直接交叉編譯到任何平台。
package main

import (
	_ "github.com/go-sql-driver/mysql"  // driver 名稱：mysql
	_ "github.com/lib/pq"               // driver 名稱：postgres
	_ "github.com/microsoft/go-mssqldb" // driver 名稱：sqlserver / mssql
	_ "modernc.org/sqlite"              // driver 名稱：sqlite（舊設定 sqlite3 會自動轉換）
)
