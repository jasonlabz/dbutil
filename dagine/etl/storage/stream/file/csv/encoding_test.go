package csv

import "testing"

func Test_gbkEncodeDecode(t *testing.T) {
	type args struct {
		src string
	}
	tests := []struct {
		name     string
		args     args
		wantDest string
		wantErr  bool
	}{
		{
			name: "1",
			args: args{
				src: "中文",
			},
			wantDest: "中文",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			src, _ := gbkEncoder(tt.args.src)
			gotDest, err := gbkDecoder(src)
			if (err != nil) != tt.wantErr {
				t.Errorf("gbkEncodeDecode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotDest != tt.wantDest {
				t.Errorf("gbkEncodeDecode() = %v, want %v", gotDest, tt.wantDest)
			}
		})
	}
}
