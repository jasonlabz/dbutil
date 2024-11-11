package csv

import (
	"golang.org/x/text/encoding/simplifiedchinese"
)

var (
	encoders = map[string]encode{
		"gbk":   gbkEncoder,
		"utf-8": utf8Encoder,
	}
	decoders = map[string]decode{
		"gbk":   gbkDecoder,
		"utf-8": utf8Decoder,
	}
)

type encode func(string) (string, error)

type decode func(string) (string, error)

func gbkDecoder(src string) (dest string, err error) {
	dest, err = simplifiedchinese.GBK.NewDecoder().String(src)
	return
}

func utf8Decoder(src string) (dest string, err error) {
	return src, nil
}

func gbkEncoder(src string) (dest string, err error) {
	dest, err = simplifiedchinese.GBK.NewEncoder().String(src)
	return
}

func utf8Encoder(src string) (dest string, err error) {
	return src, nil
}
