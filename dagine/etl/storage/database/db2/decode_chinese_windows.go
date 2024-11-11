package db2

import "golang.org/x/text/encoding/simplifiedchinese"

// db2的char和varchar类型在windows下中文字符集是gbk
func decodeChinese(data []byte) ([]byte, error) {
	return simplifiedchinese.GBK.NewDecoder().Bytes(data)
}
