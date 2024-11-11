package compress

import (
	"archive/zip"
	"io"
	"io/fs"
	"os"

	"github.com/google/uuid"
)

// ZipWriter zip压缩写入器
type ZipWriter struct {
	writer    *zip.Writer
	nowWriter io.Writer
}

// NewZipWriter 创建zip压缩写入器
func NewZipWriter(f *os.File) (zw *ZipWriter, err error) {
	var fi fs.FileInfo

	if fi, err = f.Stat(); err != nil {
		return nil, err
	}

	zw = &ZipWriter{
		writer: zip.NewWriter(f),
	}

	if zw.nowWriter, err = zw.writer.Create(fi.Name() + uuid.New().String()); err != nil {
		return nil, err
	}
	return
}

// Write 写入p
func (z *ZipWriter) Write(p []byte) (n int, err error) {
	defer z.writer.Flush()
	return z.nowWriter.Write(p)
}

// Close 关闭
func (z *ZipWriter) Close() error {
	return z.writer.Close()
}
