package com.egao.common.system.controller;

import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.egao.common.core.annotation.OperLog;
import com.egao.common.core.web.*;
import com.egao.common.core.utils.CoreUtil;
import com.egao.common.system.entity.Role;
import com.egao.common.system.service.RoleService;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 角色管理
 * Created by wangfan on 2018-12-24 16:10
 */
@Controller
@RequestMapping("/sys/role")
public class RoleController extends BaseController {
    @Autowired
    private RoleService roleService;

    @RequiresPermissions("sys:role:view")
    @RequestMapping()
    public String view() {
        return "system/role.html";
    }

    /**
     * 分页查询角色
     */
    @OperLog(value = "角色管理", desc = "分页查询")
    @RequiresPermissions("sys:role:list")
    @ResponseBody
    @RequestMapping("/page")
//    public PageResult<Role> page(HttpServletRequest request) {
    public JsonResult page(HttpServletRequest request) {
        PageParam<Role> pageParam = new PageParam<>(request);

        List<Role> records = roleService.page(pageParam, pageParam.getWrapper()).getRecords();

        PageResult<Role> rolePageResult = new PageResult<>(roleService.page(pageParam, pageParam.getWrapper()).getRecords(), pageParam.getTotal());

        JsonResult data = JsonResult.ok(0, Integer.valueOf(String.valueOf(pageParam.getTotal()))).put("data", records);

        System.out.println("reporting data:"+JSONObject.toJSON(data));
//        return rolePageResult;
        return data;
    }

    /**
     * 查询全部角色
     */
    @OperLog(value = "角色管理", desc = "查询全部")
    @RequiresPermissions("sys:role:list")
    @ResponseBody
    @RequestMapping("/list")
    public JsonResult list(HttpServletRequest request) {
        PageParam<Role> pageParam = new PageParam<>(request);
        List<Role> list = roleService.list(pageParam.getOrderWrapper());

        System.out.println("角色管理 list:"+list);
        return JsonResult.ok().setData(list);
    }

    /**
     * 根据id查询角色
     */
    @OperLog(value = "角色管理", desc = "根据id查询")
    @RequiresPermissions("sys:role:list")
    @ResponseBody
    @RequestMapping("/get")
    public JsonResult get(Integer id) {
        return JsonResult.ok().setData(roleService.getById(id));
    }

    /**
     * 添加角色
     */
    @OperLog(value = "角色管理", desc = "添加", param = false, result = true)
    @RequiresPermissions("sys:role:save")
    @ResponseBody
    @RequestMapping("/save")
    public JsonResult save(Role role) {
        if (roleService.count(new QueryWrapper<Role>().eq("role_code", role.getRoleCode())) > 0) {
            return JsonResult.error("角色标识已存在");
        }
        if (roleService.count(new QueryWrapper<Role>().eq("role_name", role.getRoleName())) > 0) {
            return JsonResult.error("角色名称已存在");
        }
        if (roleService.save(role)) {
            return JsonResult.ok("添加成功");
        }
        return JsonResult.error("添加失败");
    }

    /**
     * 修改角色
     */
    @OperLog(value = "角色管理", desc = "修改", param = false, result = true)
    @RequiresPermissions("sys:role:update")
    @ResponseBody
    @RequestMapping("/update")
    public JsonResult update(Role role) {
        if (roleService.count(new QueryWrapper<Role>().eq("role_code", role.getRoleCode())
                .ne("role_id", role.getRoleId())) > 0) {
            return JsonResult.error("角色标识已存在");
        }
        if (roleService.count(new QueryWrapper<Role>().eq("role_name", role.getRoleName())
                .ne("role_id", role.getRoleId())) > 0) {
            return JsonResult.error("角色名称已存在");
        }
        if (roleService.updateById(role)) {
            return JsonResult.ok("修改成功");
        }
        return JsonResult.error("修改失败");
    }

    /**
     * 删除角色
     */
    @OperLog(value = "角色管理", desc = "删除", result = true)
    @RequiresPermissions("sys:role:remove")
    @ResponseBody
    @RequestMapping("/remove")
    public JsonResult remove(Integer id) {
        if (roleService.removeById(id)) {
            return JsonResult.ok("删除成功");
        }
        return JsonResult.error("删除失败");
    }

    /**
     * 批量添加角色
     */
    @OperLog(value = "角色管理", desc = "批量添加", param = false, result = true)
    @RequiresPermissions("sys:role:save")
    @ResponseBody
    @RequestMapping("/saveBatch")
    public JsonResult saveBatch(@RequestBody List<Role> list) {
        // 对集合本身进行非空和重复校验
        StringBuilder sb = new StringBuilder();
        sb.append(CoreUtil.listCheckBlank(list, "roleCode", "角色标识"));
        sb.append(CoreUtil.listCheckBlank(list, "roleName", "角色名称"));
        sb.append(CoreUtil.listCheckRepeat(list, "roleCode", "角色标识"));
        sb.append(CoreUtil.listCheckRepeat(list, "roleName", "角色名称"));
        if (sb.length() != 0) return JsonResult.error(sb.toString());
        // 数据库层面校验
        if (roleService.count(new QueryWrapper<Role>().in("role_code",
                list.stream().map(Role::getRoleCode).collect(Collectors.toList()))) > 0) {
            return JsonResult.error("角色标识已存在");
        }
        if (roleService.count(new QueryWrapper<Role>().in("role_name",
                list.stream().map(Role::getRoleName).collect(Collectors.toList()))) > 0) {
            return JsonResult.error("角色名称已存在");
        }
        if (roleService.saveBatch(list)) {
            return JsonResult.ok("添加成功");
        }
        return JsonResult.error("添加失败");
    }

    /**
     * 批量删除角色
     */
    @OperLog(value = "角色管理", desc = "批量删除", result = true)
    @RequiresPermissions("sys:role:remove")
    @ResponseBody
    @RequestMapping("/removeBatch")
    public JsonResult removeBatch(@RequestBody List<Integer> ids) {
        if (roleService.removeByIds(ids)) {
            return JsonResult.ok("删除成功");
        }
        return JsonResult.error("删除失败");
    }

}
