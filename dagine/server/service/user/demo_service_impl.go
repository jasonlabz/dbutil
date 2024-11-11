package demo

import (
	"sync"

	"github.com/jasonlabz/dagine/server/service"
)

var svc *Service
var once sync.Once

func GetInstance() service.DemoService {
	if svc != nil {
		return svc
	}
	once.Do(func() {
		svc = &Service{}
	})

	return svc
}

type Service struct {
}
