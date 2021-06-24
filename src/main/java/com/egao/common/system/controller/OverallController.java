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
import com.egao.common.system.service.AdService;
import com.egao.common.system.service.CostService;
import com.egao.common.system.service.ItemsService;
import com.egao.common.system.service.OverallService;
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

/*
@Api(value = "数据报表", tags = "reporting")
@RestController
@RequestMapping("/reporting")
*/


@Controller
@RequestMapping("/sys/overall")
public class OverallController extends BaseController {


    @Autowired
    private AdService adService;

    @Autowired
    private ItemsService itemsService;

    @Autowired
    private CostService costService;

    @Autowired
    private OverallService overallService;


    @RequiresPermissions("sys:overall:view")
    @RequestMapping()
    public String view(Model model) {
        return "system/overall.html";
    }



/*
    @ApiOperation(value = "分页查询数据")
    @PreAuthorize("hasAuthority('get:/reporting')")
    @GetMapping()
//    public PageResult list(HttpServletRequest request) {
*/



    @OperLog(value = "全局管理", desc = "分页查询")
    @RequiresPermissions("sys:overall:list")
    @ResponseBody
    @RequestMapping("/page")
    public JsonResult list(HttpServletRequest request
            , @RequestParam(name = "page", required = false)Integer page, @RequestParam(name = "limit", required = false)Integer limit
            , @RequestParam(name = "itemsId", required = false)String itemsId, @RequestParam(name = "jobNumber", required = false)String jobNumber
            , @RequestParam(name = "adAccount", required = false)String adAccount, @RequestParam(name = "searchTime", required = false)String searchTime
            , @RequestParam(name = "adChannel", required = false)String adChannel) {

        PageParam pageParam = new PageParam(request);
        pageParam.setDefaultOrder(new String[]{"id"}, null);

        System.out.println("全局管理 分页查询数据...");

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


        List<Map<String, Object>> overallList = overallService.selectOverall(map);

        int overallCount = 0;
        if(overallList.size() > 0){
            overallCount = overallService.selectOverallCount(map);
        }

        JsonResult data = JsonResult.ok(0, overallCount,"成功").put("data", overallList);

        System.out.println("全局管理 data:"+JSONObject.toJSON(data));
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




/*
    @ApiOperation(value = "添加数据")
    @PreAuthorize("hasAuthority('post:/reporting')")
    @PostMapping()
*/

    /**
     * 添加数据
     */
    @OperLog(value = "全局管理", desc = "添加数据", result = true)
    @RequiresPermissions("sys:overall:add")
    @ResponseBody
    @RequestMapping("/add")
    public JsonResult add(HttpServletRequest request
            , @RequestParam(name = "adCostRatio", required = false)Float adCostRatio
            , @RequestParam(name = "logisticCostRatio", required = false)Float logisticCostRatio
            , @RequestParam(name = "goodsCostRatio", required = false)Float goodsCostRatio
            , @RequestParam(name = "operateCostRatio", required = false)Float operateCostRatio
            , @RequestParam(name = "refundRate", required = false)Float refundRate
            , @RequestParam(name = "toolCostRatio", required = false)Float toolCostRatio
            , @RequestParam(name = "passCostRatio", required = false)Float passCostRatio) {

        System.out.println("全局管理 add:"+request);

        try {
            Map map = new HashMap();
            map.put("adCostRatio", adCostRatio);
            map.put("logisticCostRatio", logisticCostRatio);
            map.put("goodsCostRatio", goodsCostRatio);
            map.put("refundRate", refundRate);
            map.put("toolCostRatio", toolCostRatio);
            map.put("passCostRatio", passCostRatio);
            map.put("operateCostRatio", operateCostRatio);
            map.put("page", 0);
            map.put("rows", 10);
            System.out.println("map："+map);

            Float required = 100f;
            Float totalCostRatio = logisticCostRatio + goodsCostRatio + operateCostRatio + refundRate + toolCostRatio + passCostRatio;
            System.out.println("totalCostRatio："+totalCostRatio);
            System.out.println("required："+required);

/*
            if(!totalCostRatio.equals(required)){
                return JsonResult.error("占比设置有误");
            }
*/


            List<Map<String, Object>> overallList = overallService.selectOverall(map);

            if(overallList.size() > 0){
                return JsonResult.error("全局成本已存在");
            }

            overallService.insertOverall(map);

            return JsonResult.ok("添加成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return JsonResult.error("添加失败");
    }


    /**
     * 修改数据
     */
    @OperLog(value = "全局管理", desc = "修改数据", result = true)
    @RequiresPermissions("sys:overall:update")
    @ResponseBody
    @RequestMapping("/update")
    public JsonResult update(HttpServletRequest request, @RequestParam(name = "id", required = false)Long id
            , @RequestParam(name = "adCostRatio", required = false)Float adCostRatio
            , @RequestParam(name = "logisticCostRatio", required = false)Float logisticCostRatio
            , @RequestParam(name = "goodsCostRatio", required = false)Float goodsCostRatio
            , @RequestParam(name = "operateCostRatio", required = false)Float operateCostRatio
            , @RequestParam(name = "refundRate", required = false)Float refundRate
            , @RequestParam(name = "toolCostRatio", required = false)Float toolCostRatio
            , @RequestParam(name = "passCostRatio", required = false)Float passCostRatio) {

        System.out.println("全局管理 update:"+request);

        try {
            Map map = new HashMap();
            map.put("id", id);
            map.put("adCostRatio", adCostRatio);
            map.put("logisticCostRatio", logisticCostRatio);
            map.put("goodsCostRatio", goodsCostRatio);
            map.put("refundRate", refundRate);
            map.put("toolCostRatio", toolCostRatio);
            map.put("passCostRatio", passCostRatio);
            map.put("operateCostRatio", operateCostRatio);
            System.out.println("map："+map);

            Float required = 100f;
            Float totalCostRatio = logisticCostRatio + goodsCostRatio + operateCostRatio + refundRate + toolCostRatio + passCostRatio;
            System.out.println("totalCostRatio："+totalCostRatio);
            System.out.println("required："+required);
            System.out.println("totalCostRatio2："+(totalCostRatio.equals(required)));

/*
            if(!totalCostRatio.equals(required)){
                return JsonResult.error("占比设置有误,没有达到100");
            }
*/


            overallService.updateOverall(map);

            return JsonResult.ok("添加成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return JsonResult.error("添加失败");
    }


    /**
     * 删除数据
     */
    @OperLog(value = "全局管理", desc = "删除数据", result = true)
    @RequiresPermissions("sys:overall:delete")
    @ResponseBody
    @RequestMapping("/delete")
    public JsonResult remove(long id) {

        System.out.println("全局管理 删除数据："+id);

        boolean result = overallService.deleteById(id);

        System.out.println("全局管理 删除数据 result："+result);

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



}
