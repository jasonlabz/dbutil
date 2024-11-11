package demo

type UserRegisterDto struct {
	Nickname string `json:"nickname"` // Comment: 用户名
	Avatar   string `json:"avatar"`   // Comment: 头像
	Password string `json:"password"` // Comment: 用户密码 des/md5加密值
	Phone    string `json:"phone"`    // Comment: 手机号 aes加密
	Gender   int32  `json:"gender"`   // Comment: 性别 0|男、1|女、9|未知
}

type UserListDto struct {
	Nickname string `json:"nickname"` // Comment: 用户名
	Avatar   string `json:"avatar"`   // Comment: 头像
	Password string `json:"password"` // Comment: 用户密码 des/md5加密值
	Phone    string `json:"phone"`    // Comment: 手机号 aes加密
	Gender   int32  `json:"gender"`   // Comment: 性别 0|男、1|女、9|未知
}
