package file

import (
	"fmt"
	"sync"

	"github.com/jasonlabz/dagine/etl/element"
	"github.com/jasonlabz/potato/jsonutil"
	"github.com/pingcap/errors"
)

// Creator 创建输出流的创建器
type Creator interface {
	Create(filename string) (stream OutStream, err error) //创建名为filename的输出流
}

// OutStream 输出流
type OutStream interface {
	Writer(conf *jsonutil.JSON) (writer StreamWriter, err error) //创建写入器
	Close() (err error)                                          //关闭输出流
}

// StreamWriter 输出流写入器
type StreamWriter interface {
	Write(record element.Record) (err error) //写入记录
	Flush() (err error)                      //刷新至文件
	Close() (err error)                      //关闭输出流写入器
}

// RegisterCreator 通过创建器名称name注册输出流创建器creator
func RegisterCreator(name string, creator Creator) {
	if err := creators.register(name, creator); err != nil {
		panic(err)
	}
}

// UnregisterAllCreater 注销所有文件打开器
func UnregisterAllCreater() {
	creators.unregisterAll()
}

// OutStreamer 输出流包装
type OutStreamer struct {
	stream OutStream
}

// NewOutStreamer 通过creator名称name的输出流包装，并打开名为filename的输出流
func NewOutStreamer(name string, filename string) (streamer *OutStreamer, err error) {
	creator, ok := creators.creator(name)
	if !ok {
		err = errors.Errorf("creator %v does not exist", name)
		return nil, err
	}
	streamer = &OutStreamer{}
	if streamer.stream, err = creator.Create(filename); err != nil {
		return nil, errors.Wrapf(err, "create fail")
	}
	return
}

// Writer 通过配置conf创建流写入器
func (s *OutStreamer) Writer(conf *jsonutil.JSON) (StreamWriter, error) {
	return s.stream.Writer(conf)
}

// Close 关闭写入包装
func (s *OutStreamer) Close() error {
	return s.stream.Close()
}

var creators = &creatorMap{
	creators: make(map[string]Creator),
}

type creatorMap struct {
	sync.RWMutex
	creators map[string]Creator
}

func (o *creatorMap) register(name string, creator Creator) error {
	if creator == nil {
		return fmt.Errorf("creator %v is nil", name)
	}

	o.Lock()
	defer o.Unlock()
	if _, ok := o.creators[name]; ok {
		return fmt.Errorf("creator %v exists", name)
	}

	o.creators[name] = creator
	return nil
}

func (o *creatorMap) creator(name string) (creator Creator, ok bool) {
	o.RLock()
	defer o.RUnlock()
	creator, ok = o.creators[name]
	return
}

func (o *creatorMap) unregisterAll() {
	o.Lock()
	defer o.Unlock()
	o.creators = make(map[string]Creator)
}
