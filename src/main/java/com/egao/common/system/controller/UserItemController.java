package com.egao.common.system.controller;

import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.egao.common.core.annotation.OperLog;
import com.egao.common.core.exception.BusinessException;
import com.egao.common.core.web.BaseController;
import com.egao.common.core.web.JsonResult;
import com.egao.common.system.entity.Menu;
import com.egao.common.system.entity.RoleMenu;
import com.egao.common.system.service.ItemsService;
import com.egao.common.system.service.MenuService;
import com.egao.common.system.service.RoleMenuService;
import com.egao.common.system.service.UserItemService;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 角色菜单管理
 * Created by wangfan on 2018-12-24 16:10
 */
@Controller
@RequestMapping("/sys/user/item")
public class UserItemController extends BaseController {

    @Autowired
    private RoleMenuService roleMenuService;

    @Autowired
    private MenuService menuService;

    @Autowired
    private ItemsService itemsService;

    @Autowired
    private UserItemService userItemService;


    /**
     * 查询角色菜单
     */
    @OperLog(value = "用户管理", desc = "查询用户站点")
//    @RequiresPermissions("sys:role:list")
    @ResponseBody
    @RequestMapping("/list")
    public JsonResult list(Integer userId) {
//        List<Menu> menus = menuService.list(new QueryWrapper<Menu>().orderByAsc("sort_number"));
//        List<RoleMenu> roleMenus = roleMenuService.list(new QueryWrapper<RoleMenu>().eq("role_id", roleId));
//        System.out.println("用户管理 roleId："+ roleId);
//        System.out.println("用户管理 menus："+ JSONObject.toJSON(menus));
//        System.out.println("用户管理 roleMenus："+ JSONObject.toJSON(roleMenus));


        Map map = new HashMap<>();
        map.put("page", 0);
        map.put("rows", 1000);
        List<Map<String, Object>> itemsList = itemsService.selectItems(map);
        List<Map<String, Object>> userItemList = userItemService.selectUserItemByUserId(userId);

        System.out.println("用户管理 userId："+ userId);
        System.out.println("用户管理 itemsList："+ JSONObject.toJSON(itemsList));
        System.out.println("用户管理 userItemList："+ JSONObject.toJSON(userItemList));






        for (Map<String, Object> itemsMap : itemsList) {
//            System.out.println("用户管理 itemsMap："+ itemsMap);

            Long itemsId = (Long)itemsMap.get("itemsId");
            itemsMap.put("checked", false);
            itemsMap.put("parentId", 1);
            itemsMap.put("hide", 0);
            itemsMap.put("open", true);
            for(Map<String, Object> userItemMap : userItemList){
                Long itemId = (Long)userItemMap.get("itemsId");
                if(itemsId.equals(Long.valueOf(itemId))){
                    itemsMap.put("checked", true);
                    break;
                }
            }
        }


        Map itemMap = new HashMap<>();
        itemMap.put("itemsId", 1L);
        itemMap.put("itemsName", "google");
        itemMap.put("checked", false);
        itemMap.put("parentId", 0);
        itemMap.put("hide", 0);
        itemMap.put("open", true);
        itemsList.add(0, itemMap);

        System.out.println("用户管理 menus2："+ JSONObject.toJSON(itemsList));
        return JsonResult.ok().setData(itemsList);
    }



    /**
     * 添加角色菜单
     */
    @OperLog(value = "角色管理", desc = "添加角色菜单")
//    @RequiresPermissions("sys:role:update")
    @ResponseBody
    @RequestMapping("/save")
    public JsonResult addRoleAuth(Integer roleId, Integer menuId) {

        System.out.println("角色管理 添加角色菜单...");


        RoleMenu roleMenu = new RoleMenu();
        roleMenu.setRoleId(roleId);
        roleMenu.setMenuId(menuId);
        if (roleMenuService.save(roleMenu)) {
            return JsonResult.ok();
        }
        return JsonResult.error();
    }


    /**
     * 移除角色菜单
     */
    @OperLog(value = "角色管理", desc = "移除角色菜单")
//    @RequiresPermissions("sys:role:update")
    @ResponseBody
    @RequestMapping("/delete")
    public JsonResult removeRoleAuth(Integer roleId, Integer menuId) {
        if (roleMenuService.remove(new UpdateWrapper<RoleMenu>()
                .eq("role_id", roleId).eq("menuId", menuId))) {
            return JsonResult.ok();
        }
        return JsonResult.error();
    }


    /**
     * 批量修改角色菜单
     */
    @OperLog(value = "角色管理", desc = "修改角色菜单")
//    @RequiresPermissions("sys:role:update")
    @Transactional
    @ResponseBody
    @RequestMapping("/update/{id}")
    public JsonResult setRoleAuth(@PathVariable("id") Integer userId, @RequestBody List<Integer> itemIds) {


        System.out.println("userId："+userId);
        System.out.println("itemIds："+itemIds);

        System.out.println("itemIds Size："+itemIds.size());


        try {
            userItemService.deleteUserItemByUserId(userId);

            if (itemIds.size() > 0) {
                for(Integer itemId : itemIds){

                    Map map = new HashMap();
                    map.put("user_id", userId);
                    map.put("item_id", itemId);
                    userItemService.insertUserItem(map);
                }

            }
            return JsonResult.ok("保存成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return JsonResult.error("保存失败");

    }

}
