package controller

import (
	"github.com/gin-gonic/gin"
	"github.com/jasonlabz/potato/consts"

	base "dagine/common/ginx"
)

// GetDemoInfo
//
//	@Summary	查询demo详情
//	@Tags		demo相关接口
//	@Accept		json
//	@Produce	json
//	@Param		demo_id	path		string			true	"demoID"
//	@Success	200		{object}	base.Response{data=string}	"ok"
//	@Router		/v1/demo/info/{demo_id} [get]
func GetDemoInfo(c *gin.Context) {
	//demoIDStr := c.Param("demo_id")
	//demoID, err := strconv.ParseInt(demoIDStr, 10, 64)
	//if err != nil {
	//	base.ResponseErr(c, consts.APIVersionV1, err)
	//}
	//demoInfo, err := demo.GetInstance().GetDemoInfo(c, demoID)
	//base.JsonResult(c, consts.APIVersionV1, demoInfo, err)
}

// RegisterDemo
//
//	@Summary	demo注册
//	@Tags		demo相关接口
//	@Accept		json
//	@Produce	json
//	@Param		demo_info	body		base.Response	true	"demo信息"
//	@Success	200			{object}	base.Response			"ok"
//	@Router		/v1/demo/register [post]
func RegisterDemo(c *gin.Context) {
	//params := &demo2.DemoRegisterDto{}
	//err := c.BindJSON(&params)
	//if err != nil {
	//	base.ResponseErr(c, consts.APIVersionV1, err)
	//	return
	//}
	//registerDemo, err := demo.GetInstance().RegisterDemo(c, params)
	//base.JsonResult(c, consts.APIVersionV1, registerDemo, err)
}

// UpdateDemoInfo
//
//	@Summary	demo信息编辑
//	@Tags		demo相关接口
//	@Accept		json
//	@Produce	json
//	@Param		update_info	body		base.Response	true	"demo信息"
//	@Success	200			{object}	base.Response				"ok"
//	@Router		/v1/demo/info [put]
func UpdateDemoInfo(c *gin.Context) {
	//params := &demo2.DemoUpdateFieldDto{}
	//err := c.BindJSON(&params)
	//if err != nil {
	//	base.ResponseErr(c, consts.APIVersionV1, err)
	//	return
	//}
	//updateDemoInfo, err := demo.GetInstance().UpdateDemoInfo(c, params)
	//base.JsonResult(c, consts.APIVersionV1, updateDemoInfo, err)
}

// DemoLogInOrLogout
//
//	@Summary	demo登录&登出
//	@Tags		demo相关接口
//	@Accept		json
//	@Produce	json
//	@Param		demo_id	path		string			true	"demoID"
//	@Param		status	query		string			true	"0|1 登录|登出"
//	@Success	200		{object}	base.Response	"ok"
//	@Router		/v1/demo/log_in_out/{demo_id} [put]
func DemoLogInOrLogout(c *gin.Context) {

	base.ResponseOK(c, consts.APIVersionV1, nil)
}

// DeleteDemo
//
//	@Summary	demo注销删除
//	@Tags		demo相关接口
//	@Accept		json
//	@Produce	json
//	@Param		demo_id	path		string			true	"demoID"
//	@Param		status	query		string			true	"0|1 登录|登出"
//	@Success	200		{object}	base.Response	"ok"
//	@Router		/v1/demo/info/{demo_id} [delete]
func DeleteDemo(c *gin.Context) {

	base.ResponseOK(c, consts.APIVersionV1, nil)
}
