package file

import (
	"errors"
	"github.com/jasonlabz/potato/jsonutil"
	"testing"

	"dagine/etl/element"
)

type mockStreamWriter struct {
	record element.Record
}

func (m *mockStreamWriter) Write(record element.Record) (err error) {
	m.record = record
	return
}

func (m *mockStreamWriter) Flush() (err error) {
	return
}

func (m *mockStreamWriter) Close() (err error) {
	return
}

type mockOutStream struct {
}

func (m *mockOutStream) Writer(conf *jsonutil.JSON) (writer StreamWriter, err error) {
	return &mockStreamWriter{}, nil
}

func (m *mockOutStream) Close() (err error) {
	return
}

type mockCreator struct {
	createErr error
}

func (m *mockCreator) Create(filename string) (stream OutStream, err error) {
	return &mockOutStream{}, m.createErr
}

func TestOutStreamer_Write(t *testing.T) {
	UnregisterAllCreater()
	RegisterCreator("mock", &mockCreator{})

	s, err := NewOutStreamer("mock", "")
	if err != nil {
		t.Fatal(err)
	}

	type args struct {
		conf *jsonutil.JSON
	}
	tests := []struct {
		name    string
		s       *OutStreamer
		args    args
		wantErr bool
	}{
		{
			name: "1",
			s:    s,
			args: args{
				conf: testJSONFromString("{}"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer tt.s.Close()
			got, err := tt.s.Writer(tt.args.conf)
			if (err != nil) != tt.wantErr {
				t.Errorf("OutStreamer.Writer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			defer got.Close()
			record := element.NewDefaultRecord()
			record.Add(element.NewDefaultColumn(element.NewStringColumnValue("mock"),
				"mock", 0))
			defer got.Flush()
			if err = got.Write(record); err != nil {
				t.Errorf("Write() error = %v", err)
				return
			}
			if c, _ := got.(*mockStreamWriter).record.GetByName("mock"); c.String() != "mock" {
				t.Errorf("InStreamer.write() fail")
				return
			}
		})
	}
}

func TestNewOutStreamerErr(t *testing.T) {
	UnregisterAllCreater()
	RegisterCreator("mockErr", &mockCreator{
		createErr: errors.New("mock errpr"),
	})

	type args struct {
		name     string
		filename string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "1",
			args: args{
				name: "mockErr",
			},
			wantErr: true,
		},
		{
			name: "2",
			args: args{
				name: "mock",
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewOutStreamer(tt.args.name, tt.args.filename)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewOutStreamer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func Test_creatorMap_registerErr(t *testing.T) {
	type args struct {
		name    string
		creator Creator
	}
	tests := []struct {
		name    string
		o       *creatorMap
		args    args
		wantErr bool
	}{
		{
			name: "1",
			o: &creatorMap{
				creators: make(map[string]Creator),
			},
			args: args{
				name:    "mock",
				creator: nil,
			},
			wantErr: true,
		},
		{
			name: "1",
			o: &creatorMap{
				creators: map[string]Creator{
					"mock": &mockCreator{},
				},
			},
			args: args{
				name:    "mock",
				creator: &mockCreator{},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.o.register(tt.args.name, tt.args.creator); (err != nil) != tt.wantErr {
				t.Errorf("creatorMap.register() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
