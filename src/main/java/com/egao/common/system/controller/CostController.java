package com.egao.common.system.controller;

import cn.hutool.core.util.StrUtil;
import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.egao.common.core.annotation.OperLog;
import com.egao.common.core.utils.AdEnum;
import com.egao.common.core.utils.CoreUtil;
import com.egao.common.core.utils.CostUtil;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.web.BaseController;
import com.egao.common.core.web.JsonResult;
import com.egao.common.core.web.PageParam;
import com.egao.common.system.entity.DictionaryData;
import com.egao.common.system.entity.Organization;
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
@RequestMapping("/sys/cost")
public class CostController extends BaseController {


    @Autowired
    private AdService adService;

    @Autowired
    private ItemsService itemsService;

    @Autowired
    private CostService costService;

    @Autowired
    private OverallService overallService;


    @Autowired
    private UserService userService;
    @Autowired
    private DictionaryDataService dictionaryDataService;
    @Autowired
    private RoleService roleService;
    @Autowired
    private OrganizationService organizationService;


    @RequiresPermissions("sys:cost:view")
    @RequestMapping()
    public String view(Model model) {

        List<Map<String, Object>> gaItemsList = itemsService.selectAllItemsByType(0);

        gaItemsList.get(0).put("selected", "selected");



        model.addAttribute("gaItemsList", JSON.toJSONString(gaItemsList));
        return "system/cost.html";
    }



    @OperLog(value = "成本管理", desc = "分页查询")
    @RequiresPermissions("sys:cost:list")
    @ResponseBody
    @RequestMapping("/page")
    public JsonResult list(HttpServletRequest request
            , @RequestParam(name = "page", required = false)Integer page, @RequestParam(name = "limit", required = false)Integer limit
            , @RequestParam(name = "itemsId", required = false)String itemsId, @RequestParam(name = "jobNumber", required = false)String jobNumber
            , @RequestParam(name = "adAccount", required = false)String adAccount, @RequestParam(name = "searchTime", required = false)String searchTime
            , @RequestParam(name = "adChannel", required = false)String adChannel) {

        PageParam pageParam = new PageParam(request);
        pageParam.setDefaultOrder(new String[]{"id"}, null);

        System.out.println("成本管理 分页查询数据...");

        System.out.println("page："+page);
        System.out.println("limit："+limit);
        System.out.println("searchTime："+searchTime);

        if(searchTime == null){

            searchTime = "";
        }


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

/*
        // 表格广告
        List<Map<String, Object>> adList = adService.selectAd(map);
        System.out.println("adList："+JSONArray.toJSON(adList));
        System.out.println("adListSize："+adList.size());

        int adCount = 0;
        Map<String, Object> totalRow = null;
        if(adList.size() > 0){

            adCount = adService.selectAdCount(map);

            // 表格全部页汇总
            totalRow = adService.selectAdSums(map);
//            adList.add(adSumMap);
        }
*/


        List<Map<String, Object>> costList = costService.selectCost(map);
        

        int costCount = 0;
//        Map<String, Object> totalRow = null;
        if(costList.size() > 0){
            costCount = costService.selectCostCount(map);
//            List<Map<String, Object>> overallList = costService.selectOverall(map);
//            totalRow = overallList.get(0);
        }

        JsonResult data = JsonResult.ok(0, costCount,"成功").put("data", costList);

        System.out.println("reporting data:"+JSONObject.toJSON(data));
        return data;
    }


    public boolean isKey(String key, Map map){

        for(Object keys : map.keySet()){
            if(key.equals(keys)){
                return true;
            }
        }
        return false;
    }





    /**
     * 添加数据
     */
    @OperLog(value = "成本管理", desc = "添加数据", result = true)
    @RequiresPermissions("sys:cost:add")
    @ResponseBody
    @RequestMapping("/add")
    public JsonResult add(HttpServletRequest request, @RequestParam(name = "itemsId", required = false)Long itemsId, @RequestParam(name = "month", required = false)String month
            , @RequestParam(name = "logisticCost", required = false)String logisticCost, @RequestParam(name = "goodsCost", required = false)String goodsCost
            , @RequestParam(name = "operateCost", required = false)String operateCost, @RequestParam(name = "refund", required = false)String refund
            , @RequestParam(name = "toolCost", required = false)String toolCost, @RequestParam(name = "passCost", required = false)String passCost) {


        System.out.println("成本管理 add:"+request);

        System.out.println("itemId:"+itemsId);
        System.out.println("month:"+month);

        try {

            Map<String, Object> costMap = costService.selectCostByMonthAndItemId(month, itemsId);
            if(costMap != null){
                return JsonResult.error("该月份的站点成本已存在");
            }


            Map map = new HashMap();
            map.put("item_id", itemsId);
            map.put("month", month);
            map.put("logisticCost", logisticCost);
            map.put("goodsCost", goodsCost);

            map.put("refund", refund);
            map.put("toolCost", toolCost);
            map.put("passCost", passCost);

            map.put("operateCost", operateCost);


            System.out.println("map："+map);

            System.out.println("map2："+map);


            costService.insertCost(map);

            return JsonResult.ok("添加成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return JsonResult.error("添加失败");
    }


    /**
     * 修改数据
     */
    @OperLog(value = "成本管理", desc = "修改数据", result = true)
    @RequiresPermissions("sys:cost:update")
    @ResponseBody
    @RequestMapping("/update")
    public JsonResult update(HttpServletRequest request, @RequestParam(name = "cost_id", required = false)Long costId
            , @RequestParam(name = "itemsId", required = false)Long itemsId, @RequestParam(name = "month", required = false)String month
            , @RequestParam(name = "logisticCost", required = false)String logisticCost, @RequestParam(name = "logisticCostRatio", required = false)String logisticCostRatio
            , @RequestParam(name = "goodsCost", required = false)String goodsCost, @RequestParam(name = "goodsCostRatio", required = false)String goodsCostRatio
            , @RequestParam(name = "operateCost", required = false)String operateCost, @RequestParam(name = "operateCostRatio", required = false)String operateCostRatio
            , @RequestParam(name = "refund", required = false)String refund, @RequestParam(name = "refundRate", required = false)String refundRate
            , @RequestParam(name = "toolCost", required = false)String toolCost, @RequestParam(name = "toolCostRatio", required = false)String toolCostRatio
            , @RequestParam(name = "passCost", required = false)String passCost, @RequestParam(name = "passCostRatio", required = false)String passCostRatio) {

        System.out.println("成本管理 update:"+request);

        System.out.println("itemsId:"+itemsId);
        System.out.println("month:"+month);


        try {

            Map<String, Object> costMap = costService.selectCostByMonthAndItemId(month, itemsId);
            if(costMap != null){
                return JsonResult.error("该月份的站点成本已存在");
            }

            Map map = new HashMap();
            map.put("costId", costId);
            map.put("item_id", itemsId);
            map.put("month", month);
            map.put("logisticCost", logisticCost);
            map.put("logisticCostRatio", logisticCostRatio);
            map.put("goodsCost", goodsCost);
            map.put("goodsCostRatio", goodsCostRatio);

            map.put("refund", refund);
            map.put("refundRate", refundRate);
            map.put("toolCost", toolCost);
            map.put("toolCostRatio", toolCostRatio);
            map.put("passCost", passCost);
            map.put("passCostRatio", passCostRatio);

            map.put("operateCost", operateCost);
            map.put("operateCostRatio", operateCostRatio);
            System.out.println("map："+map);

            costService.updateCost(map);


            return JsonResult.ok("添加成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return JsonResult.error("添加失败");
    }


    /**
     * 删除数据
     */
    @OperLog(value = "成本管理", desc = "删除数据", result = true)
    @RequiresPermissions("sys:cost:delete")
    @ResponseBody
    @RequestMapping("/delete")
    public JsonResult remove(@RequestParam(name = "cost_id", required = false)Long cost_id) {

        System.out.println("cost_id："+cost_id);

//        String month = "";
        boolean result = costService.deleteCostById(cost_id);
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



/*
    @ApiOperation(value = "获取广告账户")
    @PreAuthorize("hasAuthority('get:/reporting/getAdAccount')")
    @GetMapping("/getAdAccount")
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
     * excel导入成本
     */
    @Transactional
    @OperLog(value = "成本管理", desc = "excel导入", param = false, result = true)
    @RequiresPermissions("sys:cost:add")
    @ResponseBody
    @RequestMapping("/import")
    public JsonResult importBatch(MultipartFile file) {
        StringBuilder sb = new StringBuilder();
        try {
            // 读取excel
            int startRow = 1;
            ExcelReader reader = ExcelUtil.getReader(file.getInputStream(), 0);
            List<List<Object>> list = reader.read(startRow);
            // 进行非空和重复检查
            sb.append(CoreUtil.excelCheckBlank(list, startRow, 0, 1, 2, 3, 4, 5, 6, 7));
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
            List<Map<String, Object>> itemsList = itemsService.selectItems(itemsMap);
            System.out.println("itemsList："+JSONArray.toJSONString(itemsList));
            for (int i = 0; i < list.size(); i++) {
                List<Object> objects = list.get(i);

                String month = String.valueOf(objects.get(0));  // 月份
                String itemsName = String.valueOf(objects.get(1));  // 站点名称
                String logisticCost = String.valueOf(objects.get(2));  // 物流成本
                String goodsCost = String.valueOf(objects.get(3));  // 商品成本
                String operateCost = String.valueOf(objects.get(4));  // 运营成本
                String refund = String.valueOf(objects.get(5));  // 退款
                String toolCost = String.valueOf(objects.get(6));  // 工具成本
                String passCost = String.valueOf(objects.get(7));  // 通道成本

                Long itemsId = CostUtil.getItemsIdByName(itemsList, itemsName);
                if (itemsId == null) {
                    sb.append("第");
                    sb.append(i + startRow + 1);
                    sb.append("行第2");
                    sb.append("列站点不存在;\r\n");
                    System.out.println("itemsName："+itemsName);
                } else {
//                    user.setSex(sexDictData.getDictDataId());
                }

                Map<String, Object> costMap = costService.selectCostByMonthAndItemsName(month, itemsName);
                System.out.println("costMap："+costMap);
                if (costMap != null) {
                    sb.append("第");
                    sb.append(i + startRow + 1);
                    sb.append("行");
                    sb.append("月份和站点已存在;\r\n");
                }


                Map map = new HashMap();
                map.put("month", month);
                map.put("item_id", itemsId);
                map.put("logisticCost", logisticCost);
                map.put("goodsCost", goodsCost);
                map.put("operateCost", operateCost);
                map.put("refund", refund);
                map.put("toolCost", toolCost);
                map.put("passCost", passCost);

                System.out.println("map："+map);
                costs.add(map);
            }

            System.out.println("sb："+sb.toString());

            System.out.println("costs："+JSONArray.toJSONString(costs));
            // 开始添加用户
            int okNum = 0, errorNum = 0;
            for(Map<String, Object> costMap : costs){
                boolean result = costService.insertCost(costMap);
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
            return JsonResult.ok(sb.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return JsonResult.error("导入失败");
    }



}
