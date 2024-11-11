package compress

import (
	"archive/zip"
	"io"
	"io/fs"
	"os"
)

// ZipReader zip的读取器
type ZipReader struct {
	reader    *zip.Reader
	now       int
	nowReader io.ReadCloser
}

// NewZipReader 通过文件f创建zip读取器
func NewZipReader(f *os.File) (zr *ZipReader, err error) {
	var fi fs.FileInfo
	fi, err = f.Stat()
	if err != nil {
		return nil, err
	}
	zr = &ZipReader{}
	zr.reader, err = zip.NewReader(f, fi.Size())
	if err != nil {
		return nil, err
	}
	return zr, nil
}

// Read 传入p读取
func (z *ZipReader) Read(p []byte) (n int, err error) {
	if z.now == 0 {
		if err = z.getNowReader(); err != nil {
			return
		}
	}

	for readBytes := 0; n < len(p); {
		readBytes, err = z.nowReader.Read(p[n:])
		n += readBytes
		if err == io.EOF {
			if err = z.nowReader.Close(); err != nil {
				return
			}
			if err = z.getNowReader(); err != nil {
				return
			}
		}

		if err != nil {
			return
		}
	}
	return
}

func (z *ZipReader) getNowReader() (err error) {
	if z.now >= len(z.reader.File) {
		return io.EOF
	}

	z.nowReader, err = z.reader.File[z.now].Open()
	if err != nil {
		return err
	}
	z.now++
	return
}
