package com.egao.common.system.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.annotation.OperLog;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.web.BaseController;
import com.egao.common.core.web.JsonResult;
import com.egao.common.core.web.PageParam;
import com.egao.common.system.entity.Role;
import com.egao.common.system.entity.User;
import com.egao.common.system.service.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/sys/business")
public class BusinessController extends BaseController {


    @Autowired
    private AdService adService;

    @Autowired
    private ItemsService itemsService;

    @Autowired
    private CostService costService;

    @Autowired
    private BusinessService businessService;


    @RequiresPermissions("sys:business:view")
    @RequestMapping()
    public String view(Model model) {

        List<Map<String, Object>> gaItemsList = itemsService.selectAllItemsByType(0);

        gaItemsList.get(0).put("selected", "selected");



        model.addAttribute("gaItemsList", JSON.toJSONString(gaItemsList));
        return "system/business.html";
    }



    @OperLog(value = "商务管理平台", desc = "分页查询")
    @RequiresPermissions("sys:business:list")
    @ResponseBody
    @RequestMapping("/page")
    public JsonResult page(HttpServletRequest request
            , @RequestParam(name = "page", required = false)Integer page, @RequestParam(name = "limit", required = false)Integer limit
            , @RequestParam(name = "itemsId", required = false)String itemsId, @RequestParam(name = "jobNumber", required = false)String jobNumber
            , @RequestParam(name = "adAccount", required = false)String adAccount, @RequestParam(name = "searchTime", required = false)String searchTime
            , @RequestParam(name = "adChannel", required = false)String adChannel) {

        PageParam pageParam = new PageParam(request);
        pageParam.setDefaultOrder(new String[]{"id"}, null);

        System.out.println("商务管理平台 分页查询数据...");

        System.out.println("page："+page);
        System.out.println("limit："+limit);
        System.out.println("searchTime："+searchTime);

        searchTime = searchTime == null ? "" : searchTime;

        User loginUser = getLoginUser();
        String loginJobNumber = loginUser.getJobNumber();
        List<Role> rolesList = loginUser.getRoles();
        boolean isEmployee = true; // 是否员工
        for(Role  role : rolesList){
            if(role.getRoleId() < 3){
                isEmployee = false;
                break;
            }
        }

        String startTime = StringUtils.substringBefore(searchTime, " - ");
        // 获取7天前日期
//        startTime = StringUtils.isEmpty(startTime) ? DateUtil.timestampToTime(System.currentTimeMillis() - 86400000 * 7, "yyyy-MM") : startTime;
        // 获取一年前日期
        startTime = StringUtils.isEmpty(startTime) ? DateUtil.timestampToTime(System.currentTimeMillis() - 31536000000l, "yyyy-MM") : startTime;

        String endTime = StringUtils.substringAfter(searchTime, " - ");
        // 获取昨天日期
        endTime = StringUtils.isEmpty(endTime) ? DateUtil.timestampToTime(System.currentTimeMillis() - 86400000, "yyyy-MM") : endTime;

        if(StringUtils.isNotEmpty(startTime) && StringUtils.isNotEmpty(endTime)){
            try {

                Date date = new Date(); //获取当前的系统时间。
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM") ; //使用了默认的格式创建了一个日期格式化对象。

                Date startDate = dateFormat.parse(startTime); //注意:指定的字符串格式必须要与SimpleDateFormat的模式要一致。
                Date endDate = dateFormat.parse(endTime); //注意:指定的字符串格式必须要与SimpleDateFormat的模式要一致。

                if(startDate.getTime() > endDate.getTime()){
                    return JsonResult.error("开始日期不能大于结束日期...");
                }

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        Map map = new HashMap();
        map.put("page", (page-1)*limit);
        map.put("rows", limit);
        map.put("itemsId", itemsId);
        map.put("jobNumber", isEmployee ? loginJobNumber : jobNumber);
        map.put("adAccount", adAccount);
        map.put("adChannel", adChannel);
        map.put("startTime", startTime);
        map.put("endTime", endTime);

        System.out.println("map:"+map);

        List<Map<String, Object>> businessList = businessService.selectBusiness(map);


        int businessCount = 0;
        if(businessList.size() > 0){
            businessCount = businessService.selectBusinessCount(map);
        }

        JsonResult data = JsonResult.ok(0, businessCount,"成功").put("data", businessList);

        System.out.println("businessList data:"+JSONObject.toJSON(data));
        return data;
    }


    /**
     * 添加数据
     */
    @OperLog(value = "商务管理平台", desc = "添加数据", result = true)
    @RequiresPermissions("sys:business:add")
    @ResponseBody
    @RequestMapping("/add")
    public JsonResult add(HttpServletRequest request
            , @RequestParam(name = "business_id", required = false)String business_id, @RequestParam(name = "business_name", required = false)String business_name) {


        System.out.println("商务管理平台 add:"+request);


        try {

            Map<String, Object> businessMap = businessService.selectBusinessByBusinessId(business_id);

            if(businessMap != null){
                return JsonResult.error("该商务平台已存在");
            }

            Map map = new HashMap();

            map.put("business_id", business_id.trim());
            map.put("business_name", business_name.trim());
            map.put("status", 0);

            System.out.println("map："+map);

            businessService.insertBusiness(map);


            return JsonResult.ok("添加成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return JsonResult.error("添加失败");
    }


    /**
     * 修改数据
     */
    @OperLog(value = "商务管理平台", desc = "修改数据", result = true)
    @RequiresPermissions("sys:business:update")
    @ResponseBody
    @RequestMapping("/update")
    public JsonResult update(HttpServletRequest request, @RequestParam(name = "id", required = false)Long id
            , @RequestParam(name = "business_id", required = false)String business_id, @RequestParam(name = "business_name", required = false)String business_name) {

        System.out.println("商务管理平台 update:"+business_id);


        try {


            Map<String, Object> businessMap = businessService.selectBusinessById(id);
            if(businessMap != null){
                String businessId = (String)businessMap.get("business_id");
                if(!businessId.equals(business_id)){

                    Map<String, Object> businessMap2 = businessService.selectBusinessByBusinessId(business_id);
                    System.out.println("不是修改自己：" + businessMap2);
                    if(businessMap2 != null){
                        return JsonResult.error("该商务平台已存在");

                    }

                }else{
                    System.out.println("修改自己");
                }
            }


            Map map = new HashMap();
            map.put("id", id);
            map.put("business_id", business_id.trim());
            map.put("business_name", business_name.trim());
            map.put("status", 0);

            System.out.println("map:"+map);
            businessService.updateBusiness(map);


            return JsonResult.ok("添加成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return JsonResult.error("添加失败");
    }


    /**
     * 删除数据
     */
    @OperLog(value = "商务管理平台", desc = "删除数据", result = true)
    @RequiresPermissions("sys:business:delete")
    @ResponseBody
    @RequestMapping("/delete")
    public JsonResult remove(Long id) {

        System.out.println("商务管理平台 删除数据："+id);

        businessService.deleteById(id);


        return JsonResult.ok("删除成功");
    }




}
