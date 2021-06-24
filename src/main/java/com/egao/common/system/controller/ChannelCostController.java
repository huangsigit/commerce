package com.egao.common.system.controller;

import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.annotation.OperLog;
import com.egao.common.core.utils.CoreUtil;
import com.egao.common.core.utils.CostUtil;
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
import org.springframework.transaction.annotation.Transactional;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/*
@Api(value = "数据报表", tags = "reporting")
@RestController
@RequestMapping("/reporting")
*/


@Controller
@RequestMapping("/sys/channelCost")
public class ChannelCostController extends BaseController {


    @Autowired
    private AdService adService;

    @Autowired
    private ItemsService itemsService;

    @Autowired
    private CostService costService;

    @Autowired
    private ChannelCostService channelCostService;

    @Autowired
    private OverallService overallService;


    @Autowired
    private ChannelService channelService;

    @Autowired
    private UserItemService userItemService;


    @RequiresPermissions("sys:channelCost:view")
    @RequestMapping()
    public String view(Model model) {

        User loginUser = getLoginUser();
        Integer userId = loginUser.getUserId();

        List<Map<String, Object>> gaItemsList = itemsService.selectItemsByUserIdAndType(userId, 0);

        List<Map<String, Object>> channelList = channelService.selectChannelList(new HashMap());

        System.out.println("JSON.toJSONString(gaItemsList)："+JSON.toJSONString(gaItemsList));
        model.addAttribute("itemsList", JSON.toJSON(gaItemsList));
        model.addAttribute("gaItemsList", JSON.toJSONString(gaItemsList));
        model.addAttribute("channelList", JSON.toJSONString(channelList));

        return "system/channelCost.html";
    }



    @OperLog(value = "渠道成本", desc = "分页查询")
    @RequiresPermissions("sys:channelCost:list")
    @ResponseBody
    @RequestMapping("/page")
    public JsonResult list(HttpServletRequest request
            , @RequestParam(name = "page", required = false)Integer page, @RequestParam(name = "limit", required = false)Integer limit, @RequestParam(name = "channelId", required = false)Integer channelId
            , @RequestParam(name = "itemsId", required = false)String itemsId, @RequestParam(name = "jobNumber", required = false)String jobNumber
            , @RequestParam(name = "adAccount", required = false)String adAccount, @RequestParam(name = "searchTime", required = false)String searchTime
            , @RequestParam(name = "adChannel", required = false)String adChannel) {

        PageParam pageParam = new PageParam(request);
        pageParam.setDefaultOrder(new String[]{"id"}, null);

        System.out.println("渠道成本管理 分页查询数据...");

        System.out.println("page："+page);
        System.out.println("limit："+limit);
        System.out.println("searchTime："+searchTime);
        System.out.println("itemsId："+itemsId);

        if(searchTime == null){
            searchTime = "";
        }

        User loginUser = getLoginUser();
        Integer userId = loginUser.getUserId();

        Integer loginUserId = getLoginUserId();
        System.out.println("userId："+userId);
        System.out.println("loginUserId："+loginUserId);

        String loginJobNumber = loginUser.getJobNumber();

        List<Role> rolesList = loginUser.getRoles();

        boolean isEmployee = false; // 是否员工

        String startTime = StringUtils.substringBefore(searchTime, " - ");

        System.out.println("startTime1："+startTime);
        System.out.println("startTime11："+(System.currentTimeMillis() - 86400000*30l));
        // 获取7天前日期
//        startTime = StringUtils.isEmpty(startTime) ? DateUtil.timestampToTime(System.currentTimeMillis() - 86400000 * 7, "yyyy-MM") : startTime;
        // 获取一年前日期
        startTime = StringUtils.isEmpty(startTime) ? DateUtil.timestampToTime(System.currentTimeMillis() - 86400000*30l, "yyyy-MM-dd") : startTime;
        System.out.println("startTime2："+startTime);

        String endTime = StringUtils.substringAfter(searchTime, " - ");
        // 获取昨天日期
        endTime = StringUtils.isEmpty(endTime) ? DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd") : endTime;

        if(StringUtils.isNotEmpty(startTime) && StringUtils.isNotEmpty(endTime)){
            try {

                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd") ; //使用了默认的格式创建了一个日期格式化对象。

                Date startDate = dateFormat.parse(startTime); //注意:指定的字符串格式必须要与SimpleDateFormat的模式要一致。
                Date endDate = dateFormat.parse(endTime); //注意:指定的字符串格式必须要与SimpleDateFormat的模式要一致。

                if(startDate.getTime() > endDate.getTime()){
                    System.out.println("startDate.getTime()："+startDate.getTime());
                    System.out.println("endDate.getTime()："+endDate.getTime());

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
        map.put("channelId", channelId);
        map.put("jobNumber", isEmployee ? loginJobNumber : jobNumber);
        map.put("adAccount", adAccount);
        map.put("adChannel", adChannel);
        map.put("userId", userId);
        map.put("startTime", startTime);
        map.put("endTime", endTime);

        System.out.println("map:"+map);


        List<Map<String, Object>> costList = channelCostService.selectChannelCost(map);


        int costCount = 0;
        if(costList.size() > 0){
//            costCount = costService.selectCostCount(map);
            costCount = channelCostService.selectChannelCostCount(map);

        }

        JsonResult data = JsonResult.ok(0, costCount,"成功").put("data", costList);

        System.out.println("渠道成本 data:"+JSONObject.toJSON(data));
        return data;
    }








    /**
     * 添加数据
     */
    @OperLog(value = "渠道成本管理", desc = "添加数据", result = true)
    @RequiresPermissions("sys:channelCost:add")
    @ResponseBody
    @RequestMapping("/add")
    public JsonResult add(HttpServletRequest request, @RequestParam(name = "month", required = false)String month, @RequestParam(name = "dates", required = false)String dates
            , @RequestParam(name = "itemsId", required = false)Long itemsId, @RequestParam(name = "channelId", required = false)Long channelId
            , @RequestParam(name = "cost", required = false)String costStr) {

        System.out.println("渠道成本管理 add:"+request);

        System.out.println("itemId:"+itemsId);
        System.out.println("channelId:"+channelId);
        System.out.println("month:"+month);
        System.out.println("adCost:"+costStr);




        try {

            BigDecimal cost = new BigDecimal(costStr);
            BigDecimal zero = new BigDecimal(0.00);
            if(cost.compareTo(zero) < 1){
                System.out.println("cost小于等于0："+cost);
                return JsonResult.error("成本不能小于等于0");
            }


            Map map = new HashMap();
            map.put("item_id", itemsId);
            map.put("channel_id", channelId);
//            map.put("month", month);
            map.put("dates", dates);
            map.put("cost", cost);

            System.out.println("map："+map);


            Map<String, Object> channelCostMap = channelCostService.selectChannelCostByItemIdAndChannelId(month, itemsId, channelId);
            if(channelCostMap != null){
                return JsonResult.error("该成本已存在");
            }



            Integer userId = getLoginUserId();
            List<Map<String, Object>> userItemList = userItemService.selectUserItemByUserId(userId);
            map.put("userItemList", userItemList);
            map.put("userId", userId);

            channelCostService.insertChannelCost(map);

            return JsonResult.ok("添加成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return JsonResult.error("添加失败");
    }



    /**
     * 修改数据
     */
    @OperLog(value = "渠道成本管理", desc = "修改数据", result = true)
    @RequiresPermissions("sys:channelCost:update")
    @ResponseBody
    @RequestMapping("/update")
    public JsonResult update(HttpServletRequest request, @RequestParam(name = "cost_id", required = false)Long costId, @RequestParam(name = "channelId", required = false)Long channelId
            , @RequestParam(name = "itemsId", required = false)Long itemsId, @RequestParam(name = "dates", required = false)String dates
            , @RequestParam(name = "cost", required = false)String costStr) {

        System.out.println("成本管理 update:"+request);

        System.out.println("itemsId:"+itemsId);
        System.out.println("dates:"+dates);



        try {

            BigDecimal cost = new BigDecimal(costStr);
            BigDecimal zero = new BigDecimal(0.00);
            if(cost.compareTo(zero) < 1){
                System.out.println("adCost小于等于0："+cost);
                return JsonResult.error("成本不能小于等于0");
            }


            Map map = new HashMap();
            map.put("costId", costId);
            map.put("item_id", itemsId);
            map.put("channel_id", channelId);
            map.put("dates", dates);
            map.put("cost", cost);
            System.out.println("map："+map);


            Map<String, Object> channelCostMap = channelCostService.selectChannelCostByItemIdAndChannelId(dates, itemsId, channelId);

            if(channelCostMap != null){
                Long cost_id = (Long)channelCostMap.get("cost_id");
                System.out.println("database cost_id："+cost_id);
                System.out.println("costId："+costId);

                if(!cost_id.equals(costId)){
                    return JsonResult.error("该成本已存在");
                }
            }


            Integer userId = getLoginUserId();
            List<Map<String, Object>> userItemList = userItemService.selectUserItemByUserId(userId);
            map.put("userItemList", userItemList);
            map.put("userId", userId);
            channelCostService.updateChannelCost(map);


            return JsonResult.ok("添加成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return JsonResult.error("添加失败");
    }


    /**
     * 删除数据
     */
    @OperLog(value = "渠道成本管理", desc = "删除数据", result = true)
    @RequiresPermissions("sys:channelCost:delete")
    @ResponseBody
    @RequestMapping("/delete")
    public JsonResult remove(Integer cost_id) {

        System.out.println("成本管理 删除数据cost_id："+cost_id);

//        boolean result = channelCostService.deleteByMonth(month);
        boolean result = channelCostService.deleteChannelCostById(cost_id);
        System.out.println("成本管理 删除数据 result："+result);

        if (result) {
            return JsonResult.ok("删除成功");
        }
        return JsonResult.error("删除失败");
    }





/*
    @ApiOperation(value = "修改数据")
    @PreAuthorize("hasAuthority('put:/reporting')")
    @PutMapping()
    public JsonResult update(LZGame game) {
        System.out.println("update game:"+game);

        if (gameService.updateById(game)) {
            return JsonResult.ok("修改成功！");
        }
        return JsonResult.error("修改失败！");
    }
*/


/*
    @ApiOperation(value = "删除数据")
    @PreAuthorize("hasAuthority('delete:/reporting/{id}')")
    @DeleteMapping("/{id}")
    public JsonResult delete(@PathVariable("id") Integer gameId) {
        if (gameService.removeById(gameId)) {
            return JsonResult.ok("删除成功");
        }
        return JsonResult.error("删除失败");
    }
*/

/*
    @ApiOperation(value = "查询所有数据")
    @PreAuthorize("hasAuthority('get:/reporting/all')")
    @GetMapping("/all")
    public JsonResult listAll() {

        return JsonResult.ok().put("data", gameService.list());
    }
*/


    /*@ApiOperation(value = "获取站点")
    @PreAuthorize("hasAuthority('get:/reporting/getSite')")
    @GetMapping("/getSite")*/
//    public Map getSite(HttpServletRequest request) {

    /**
     * 获取站点
     */
    @OperLog(value = "站点数据", desc = "获取站点", param = false, result = true)
    @RequiresPermissions("sys:cost:list")
    @ResponseBody
    @RequestMapping("/getSite")
    public JsonResult getSite(HttpServletRequest request) {

        System.out.println("获取站点 list："+request);

/*
        String siteStr = redisTemplate.opsForValue().get("items");
//        String siteStr = redisTemplate.opsForValue().get("itemsList");
        System.out.println("siteStr："+siteStr);
*/



//        JSONObject siteJson = (JSONObject)JSONObject.toJSON(siteStr);





/*
        JSONObject siteJson = JSONObject.parseObject(siteStr);
        System.out.println("siteJson:"+siteJson);

        List list = new ArrayList();
        JSONArray itemsArr = siteJson.getJSONArray("items");
        for(int i = 0; i < itemsArr.size(); i++){
            JSONObject item = itemsArr.getJSONObject(i);
            String id = item.getString("id");
            String name = item.getString("name");

            Map map = new HashMap<>();
            map.put("id", id);
            map.put("name", name);
            list.add(map);
        }
*/


/*
        Map data = new HashMap();
        data.put("code", 200);
        data.put("data", siteStr);
        data.put("msg", "操作成功");
        System.out.println("data："+data);
*/


        Map map = new HashMap();
        map.put("page", 0);
        map.put("rows", 100);

        List<Map<String, Object>> itemsList = itemsService.selectItems(map);

        JsonResult data = JsonResult.ok().put("data", JSONObject.toJSONString(itemsList));
        System.out.println("data222："+data);
        return data;



/*
        PageParam pageParam = new PageParam(request);
        pageParam.setDefaultOrder(new String[]{"id"}, null);
//        return menuService.listFull(pageParam);

        System.out.println("lzgame list pageParam："+pageParam);
        PageResult<LZGame> lzGamePageResult = gameService.listFull(pageParam);

        lzGamePageResult.getData().get(0).setGpStatusStr("");
        System.out.println("gameService.listFull(pageParam)："+lzGamePageResult);


        for(int i = 0; i < lzGamePageResult.getData().size(); i++){
            LZGame lzGame = lzGamePageResult.getData().get(i);

            Integer id = lzGame.getId();
            Integer gpStatus = lzGame.getGpStatus();

        }

        return lzGamePageResult;
*/

    }



/*

    */
/**
     * 获取工号
     *//*

    @OperLog(value = "站点数据", desc = "获取工号", param = false, result = true)
    @RequiresPermissions("sys:cost:list")
    @ResponseBody
    @RequestMapping("/getJob")
    public JsonResult getJob(HttpServletRequest request) {

        System.out.println("获取工号");
        String itemsId = request.getParameter("itemsId");
        System.out.println("itemsId:"+itemsId);

//        List<Map<String, Object>> itemsList = jobNumberService.selectJobNumberByItemsId(itemsId);
        List<Map<String, Object>> itemsList = adService.selectAdGroupByJobNumber(Long.valueOf(itemsId));
        System.out.println("itemsList:"+itemsList);

//        String data2 = AnalyticsUtil.getData("206036759", "ostudio01@ostudio01.iam.gserviceaccount.com", "ostudio01-788809f30767.p12");
//        System.out.println("data2："+data2);


        JsonResult data = JsonResult.ok().put("data", itemsList);

        return data;
    }
*/




    /**
     * 获取广告账户
     */
    @OperLog(value = "站点数据", desc = "获取广告账户", param = false, result = true)
    @RequiresPermissions("sys:cost:list")
    @ResponseBody
    @RequestMapping("/getAdAccount")
    public JsonResult getADAccount(HttpServletRequest request) {

        System.out.println("获取广告账户");
        String itemsIdStr = request.getParameter("itemsId");
        String jobNumber = request.getParameter("jobNumber");

        Long itemsId = Long.valueOf(itemsIdStr);
//        List<Map<String, Object>> adAccountList = adService.selectAdAccountByItemsIdAndJobNumber(itemsId, jobNumber);
        List<Map<String, Object>> adAccountList = adService.selectAdGroupByAdAccount(itemsId, jobNumber);

        System.out.println("itemsId:"+itemsId);
        System.out.println("jobNumber:"+jobNumber);
        System.out.println("adAccountList:"+adAccountList);

        JsonResult data = JsonResult.ok().put("data", adAccountList);

        return data;
    }


    /**
     * 获取收入
     */
    @OperLog(value = "渠道成本", desc = "获取收入", param = false, result = true)
    @RequiresPermissions("sys:items:list")
    @ResponseBody
    @RequestMapping("/getRevenue")
    public JsonResult getRevenue(HttpServletRequest request
            , @RequestParam(name = "month", required = false)String month, @RequestParam(name = "channelId", required = false)Long channelId
            , @RequestParam(name = "itemsId", required = false)Long itemsId) {

        System.out.println("渠道成本 获取收入 month：" + month);
        System.out.println("获取收入 itemsId：" + itemsId);
        System.out.println("获取收入 channelId：" + channelId);

        Date date = DateUtil.parseDate(month, "yyyy-MM");

        System.out.println("获取收入 date.getTime()：" + date.getTime());


/*
        if(true){
            return JsonResult.error("修改失败！");
        }
*/

/*
        if(itemsId == null){
            System.out.println("没有itemsId");
            List<Map<String, Object>> itemList = itemsService.selectItemByParentId(0L);

            if(itemList.size() > 0){
                itemsId = (Long)itemList.get(0).get("itemsId");
            }
        }
*/


        Map map = new HashMap();
        map.put("startTime", DateUtil.getAMonthFirstDay(date.getTime()));
        map.put("endTime", DateUtil.getAMonthLastDay(date.getTime()));
        map.put("itemsId", itemsId);
        map.put("channelId", channelId);

        System.out.println("map："+map);
//        Float revenue = adService.selectAMonthRevenue(map);
        Float revenue = channelCostService.selectRevenue(map);

        System.out.println("revenue："+revenue);

        map = new HashMap();
//        revenue = 1000f;
        map.put("revenue", revenue == null ? 0.00 : revenue);

        JsonResult data = JsonResult.ok().put("data", map);
        System.out.println("getRevenue data2："+JSONObject.toJSON(data));
        return data;

    }


    /**
     * excel导入成本
     */
    @Transactional
    @OperLog(value = "渠道成本", desc = "excel导入", param = false, result = true)
    @ResponseBody
    @RequestMapping("/import")
    public JsonResult importBatch(MultipartFile file) {

        System.out.println("渠道成本导入开始："+System.currentTimeMillis());
        StringBuilder sb = new StringBuilder();
        try {

            Integer userId = getLoginUserId();


            // 读取excel
            int startRow = 1;
            ExcelReader reader = ExcelUtil.getReader(file.getInputStream(), 0);
            System.out.println("reader:"+reader);


            List<List<Object>> list = reader.read(startRow);
            System.out.println("list:"+list);
            // 进行非空和重复检查
            sb.append(CoreUtil.excelCheckBlank(list, startRow, 0, 1, 2, 3));
//            sb.append(CoreUtil.excelCheckRepeat(list, startRow, 0, 5, 6));
            if (!sb.toString().isEmpty()) return JsonResult.error(sb.toString());
            // 进行数据库层面检查
            List<User> users = new ArrayList<>();
            List<Map<String, Object>> costs = new ArrayList<>();
            Map itemsMap = new HashMap();
            itemsMap.put("keyword", "");
            itemsMap.put("page", 0);
            itemsMap.put("rows", 1000);
//            costService.selectCost(costMap);

            if(true){
                return JsonResult.error("导入失败");
            }


            List<Map<String, Object>> itemsList = itemsService.selectItems(itemsMap);
            System.out.println("itemsList："+JSONArray.toJSONString(itemsList));

            List<Map<String, Object>> channelList = channelService.selectChannel(itemsMap);
            System.out.println("channelList："+JSONArray.toJSONString(channelList));

            List<Map<String, Object>> userItemList = userItemService.selectUserItemByUserId(userId);
            System.out.println("userItemList："+JSONArray.toJSONString(userItemList));

            for (int i = 0; i < list.size(); i++) {
                List<Object> objects = list.get(i);

                String dates = String.valueOf(objects.get(0));  // 日期
                String itemsName = String.valueOf(objects.get(1));  // 站点名称
                String channelName = String.valueOf(objects.get(2));  // 渠道名称
                String channelCost = String.valueOf(objects.get(3));  // 渠道成本

                Long itemsId = CostUtil.getItemsIdByName(itemsList, itemsName);
                if (itemsId == null) {
                    sb.append("第");
                    sb.append(i + startRow + 1);
                    sb.append("行第2");
                    sb.append("列站点不存在;\r\n");
                    System.out.println("itemsName no exist："+itemsName);
                }

                boolean isAuth = isAuth(userItemList, Integer.valueOf(String.valueOf(itemsId)), userId);
                if (isAuth == false) {
                    sb.append("第");
                    sb.append(i + startRow + 1);
                    sb.append("行第2");
                    sb.append("列站点无权限操作;\r\n");
                    System.out.println("itemsId no auth："+itemsId);
                }


                Long channelId = CostUtil.getChannelIdByName(channelList, channelName);
                System.out.println("channelId："+channelId);
                if (channelId == null) {
                    sb.append("第");
                    sb.append(i + startRow + 1);
                    sb.append("行第3");
                    sb.append("列渠道不存在;\r\n");
                    System.out.println("channelName no exist："+channelName);
                }

                Map<String, Object> costMap = channelCostService.selectChannelCostByItemIdAndChannelId(dates, itemsId, channelId);
                System.out.println("costMap："+costMap);
                if (costMap != null) {
                    sb.append("第");
                    sb.append(i + startRow + 1);
                    sb.append("行");
                    sb.append("站点和渠道已存在;\r\n");
                }


                Map map = new HashMap();
                map.put("dates", dates);
                map.put("item_id", itemsId);
                map.put("channelName", channelName);
                map.put("channel_id", channelId);
                map.put("cost", channelCost);
                map.put("userItemList", userItemList);
                map.put("userId", userId);

                System.out.println("map："+map);
                costs.add(map);
            }

            System.out.println("sb："+sb.toString());

            System.out.println("costs："+ JSONArray.toJSONString(costs));
            // 开始添加用户
            int okNum = 0, errorNum = 0;
            for(Map<String, Object> costMap : costs){
                boolean result = channelCostService.insertChannelCost(costMap);
                System.out.println("result："+result + " costMap："+costMap);
                if (result){
                    okNum++;
                } else {
                    errorNum++;
                }
            }

            sb.append("导入完成，成功" + okNum + "条，失败" + errorNum + "条\n");

            System.out.println("导入完成，成功" + okNum + "条，失败" + errorNum + "条");
//            return JsonResult.ok("导入完成，成功" + okNum + "条，失败" + errorNum + "条");

            System.out.println("渠道成本导入结束："+System.currentTimeMillis());
            return JsonResult.ok(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return JsonResult.error("导入失败");
    }

    public boolean isAuth(List<Map<String, Object>> userItemList, Integer itemsId, Integer userId){
        for(Map<String, Object> userItemMap : userItemList){
            Integer user_id = (Integer)userItemMap.get("user_id");
            Integer item_id = (Integer)userItemMap.get("item_id");
            if(itemsId.equals(item_id) && userId.equals(user_id)){
                return true;
            }
        }
        return false;
    }

}
