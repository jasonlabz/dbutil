package demo

import "time"

type UserResDto struct {
	UserID       int64     `json:"user_id"`       // Comment: 用户ID
	UserName     string    ` json:"user_name"`    // Comment: 用户名
	Gender       int32     `json:"gender"`        // Comment: 性别 0|男、1|女、9|未知
	RegisterIP   string    `json:"register_ip"`   // Comment: no comment
	RegisterTime time.Time `json:"register_time"` // Comment: no comment
}

type UserInfoDto struct {
	UserID       int64     `json:"user_id"`       // Comment: 用户ID
	UserName     string    ` json:"user_name"`    // Comment: 用户名
	Phone        string    ` json:"phone"`        // Comment: 手机号
	Gender       int32     `json:"gender"`        // Comment: 性别 0|男、1|女、9|未知
	RegisterIP   string    `json:"register_ip"`   // Comment: no comment
	RegisterTime time.Time `json:"register_time"` // Comment: no comment
}

type UserUpdateFieldDto struct {
	UserID   int64   `json:"user_id" required:"true"` // Comment: 用户ID
	UserName *string ` json:"user_name"`              // Comment: 用户名
	Gender   *int32  `json:"gender"`                  // Comment: 性别 0|男、1|女、9|未知
	Phone    *string `json:"phone"`                   // Comment: 手机号
	Password *string `json:"password"`                // Comment: 密码
}
