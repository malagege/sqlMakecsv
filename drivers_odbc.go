//go:build windows || odbc

// ODBC 驅動：Windows 上走系統呼叫（不需 CGO）預設內建；
// Linux / macOS 需要 unixODBC 與 CGO，請用 go build -tags odbc 自行加入。
package main

import (
	_ "github.com/alexbrainman/odbc" // driver 名稱：odbc
)
