package sqlserver

import (
	"reflect"
	"testing"
)

func TestNewConfig(t *testing.T) {
	type args struct {
		conf *jsonutil.JSON
	}
	tests := []struct {
		name    string
		args    args
		wantC   *Config
		wantErr bool
	}{
		{
			name: "1",
			args: args{
				conf: testJSONFromString(`{
					"url":"sqlserver://127.0.0.1:1234/instance",
					"username":"user",
					"password":"passwd"
				}`),
			},
			wantC: &Config{
				URL:      "sqlserver://127.0.0.1:1234/instance",
				Username: "user",
				Password: "passwd",
			},
		},
		{
			name: "2",
			args: args{
				conf: testJSONFromString(`{
					"url":"sqlserver://127.0.0.1:1234/instance",
					"username":"user",
					"password":1
				}`),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotC, err := NewConfig(tt.args.conf)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewConfig() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotC, tt.wantC) {
				t.Errorf("NewConfig() = %v, want %v", gotC, tt.wantC)
			}
		})
	}
}
func TestConfig_FormatDSN(t *testing.T) {
	tests := []struct {
		name    string
		c       *Config
		wantDsn string
		wantErr bool
	}{
		{
			name: "1",
			c: &Config{
				URL:      "sqlserver://127.0.0.1:1234/instance?disableRetry=true",
				Username: "user",
				Password: "passwd",
			},
			wantDsn: "sqlserver://user:passwd@127.0.0.1:1234/instance?disableRetry=true",
		},
		{
			name: "2",
			c: &Config{
				URL:      "sqlserver://127.0.0.1:1xxx/instance",
				Username: "user",
				Password: "passwd",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDsn, err := tt.c.FormatDSN()
			if (err != nil) != tt.wantErr {
				t.Errorf("Config.FormatDSN() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotDsn != tt.wantDsn {
				t.Errorf("Config.FormatDSN() = %v, want %v", gotDsn, tt.wantDsn)
			}
		})
	}
}
