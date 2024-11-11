package db2

// db2的char和varchar类型在linux下中文字符集是utf-8
func decodeChinese(data []byte) ([]byte, error) {
	return data, nil
}
