package db2

import (
	"reflect"
	"testing"

	"dagine/etl/element"
	"dagine/etl/storage/database"
	"golang.org/x/text/encoding/simplifiedchinese"
)

func gbk(data []byte) []byte {
	v, err := simplifiedchinese.GBK.NewEncoder().Bytes(data)
	if err != nil {
		panic(err)
	}
	return v
}

func TestScanner_Scan_Chinese(t *testing.T) {
	type args struct {
		src interface{}
	}
	tests := []struct {
		name    string
		s       *Scanner
		args    args
		wantErr bool
		want    element.Column
	}{
		//"CHAR"  "VARCHAR"
		{
			name: "CHAR",
			s:    NewScanner(NewField(database.NewBaseField(0, "test", newMockFieldType("CHAR")))),
			args: args{
				src: gbk([]byte("中文abc")),
			},
			want: element.NewDefaultColumn(element.NewStringColumnValue("中文abc"), "test", element.ByteSize(gbk([]byte("中文abc")))),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.s.Scan(tt.args.src); (err != nil) != tt.wantErr {
				t.Errorf("Scanner.Scan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(tt.s.Column(), tt.want) {
				t.Errorf("Scanner.Column() = %v, want %v", tt.s.Column(), tt.want)
			}
		})
	}
}
