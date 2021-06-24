package com.egao.common.system.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.Cache;
import com.egao.common.core.Constants;
import com.egao.common.core.FBConstants;
import com.egao.common.core.UploadConstant;
import com.egao.common.core.annotation.OperLog;
import com.egao.common.core.utils.AdEnum;
import com.egao.common.core.utils.AnalyticsUtil;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.utils.HttpUtil;
import com.egao.common.core.web.BaseController;
import com.egao.common.core.web.JsonResult;
import com.egao.common.core.web.PageParam;
import com.egao.common.system.entity.Role;
import com.egao.common.system.entity.User;
import com.egao.common.system.service.AdService;
import com.egao.common.system.service.CertificateService;
import com.egao.common.system.service.ChannelService;
import com.egao.common.system.service.ItemsService;
import com.google.api.services.analytics.model.AccountSummary;
import com.google.api.services.analytics.model.ProfileSummary;
import com.google.api.services.analytics.model.WebPropertySummary;
import org.apache.commons.lang3.StringUtils;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/*
@Api(value = "数据报表", tags = "reporting")
@RestController
@RequestMapping("/reporting")
*/


@Controller
@RequestMapping("/sys/itemsData1016")
public class ItemsDataController1016 extends BaseController {

//    @Autowired
//    private LZGameService gameService;


    @Autowired
    private AdService adService;

    @Autowired
    private ItemsService itemsService;

    @Autowired
    private CertificateService certificateService;

    @Autowired
    private ChannelService channelService;


    @RequiresPermissions("sys:itemsData:view")
    @RequestMapping()
    public String view(Model model) {

        List<Map<String, Object>> channelList = channelService.selectChannelList(new HashMap());

        Map map = new HashMap();
        map.put("value", "0");
        map.put("name", "其它");
        channelList.add(map);

        model.addAttribute("channelList", JSON.toJSONString(channelList));

        return "system/itemsData.html";
    }


    @OperLog(value = "站点数据", desc = "分页查询")
    @RequiresPermissions("sys:itemsData:list")
    @ResponseBody
    @RequestMapping("/page")
    public JsonResult list(HttpServletRequest request
            , @RequestParam(name = "page", required = false)Integer page, @RequestParam(name = "limit", required = false)Integer limit
            , @RequestParam(name = "itemsId", required = false)String itemsId, @RequestParam(name = "jobNumber", required = false)String jobNumber
            , @RequestParam(name = "adAccount", required = false)String adAccount, @RequestParam(name = "searchTime", required = false)String searchTime
            , @RequestParam(name = "adChannel", required = false)String adChannel, @RequestParam(name = "channelId", required = false)String channelId) {


        try {
            PageParam pageParam = new PageParam(request);
            pageParam.setDefaultOrder(new String[]{"id"}, null);


            System.out.println("站点数据 分页查询数据...");

            System.out.println("page："+(page-1)*limit);
            System.out.println("limit："+limit);
            System.out.println("searchTime："+searchTime);


//            PageHelper.startPage((page-1)*limit, limit + (page-1)*limit);


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
            startTime = StringUtils.isEmpty(startTime) ? DateUtil.timestampToTime(System.currentTimeMillis() - 86400000 * 7, "yyyy-MM-dd") : startTime;

            String endTime = StringUtils.substringAfter(searchTime, " - ");
            // 获取昨天日期
            endTime = StringUtils.isEmpty(endTime) ? DateUtil.timestampToTime(System.currentTimeMillis() - 86400000, "yyyy-MM-dd") : endTime;

            if(StringUtils.isNotEmpty(startTime) && StringUtils.isNotEmpty(endTime)){
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd") ; //使用了默认的格式创建了一个日期格式化对象。
                Date startDate = dateFormat.parse(startTime); //注意:指定的字符串格式必须要与SimpleDateFormat的模式要一致。
                Date endDate = dateFormat.parse(endTime); //注意:指定的字符串格式必须要与SimpleDateFormat的模式要一致。
                if(startDate.getTime() > endDate.getTime()){
                    return JsonResult.error("开始日期不能大于结束日期...");
                }
            }


            Map map = new HashMap();
            map.put("page", (page-1)*limit);
            map.put("rows", limit);
//        map.put("rows", 100);
            map.put("itemsId", itemsId);
            map.put("jobNumber", isEmployee ? loginJobNumber : jobNumber);
            map.put("adAccount", adAccount);
            map.put("adChannel", adChannel);
            map.put("startTime", startTime);
            map.put("endTime", endTime);
            map.put("channelId", channelId);


            Map map2 = new HashMap();
            map2.put("page", (page-1)*limit);
            map2.put("rows", limit);
//        map.put("rows", 100);
            map2.put("itemsId", itemsId);
            map2.put("jobNumber", isEmployee ? loginJobNumber : jobNumber);
            map2.put("adAccount", adAccount);
            map2.put("adChannel", adChannel);
            map2.put("startTime", startTime);
            map2.put("endTime", endTime);
            map2.put("channelId", channelId);



            System.out.println("map:"+map);


            // 表格广告
//            List<Map<String, Object>> adList = adService.selectAd(map);
//            List<Map<String, Object>> adList = adService.selectItemData(map);
            List<Map<String, Object>> adList = itemsService.selectItemData(map);

            System.out.println("adList："+JSONArray.toJSON(adList));
            System.out.println("adListSize："+adList.size());

            int adCount = 0;
            Map<String, Object> totalRow = null;
            if(adList.size() > 0){

                for(Map<String, Object> adMap : adList){
                    Long itemId = (Long)adMap.get("itemsId");
                    String itemsName = (String)adMap.get("itemsName");
                    if(itemsName == null){
                        String itemsNameCache = Cache.getItemsName(String.valueOf(itemId));
                        adMap.put("itemsName", itemsNameCache);
                    }



                }


                adCount = adService.selectAdCount(map);
                System.out.println("-------adCount："+adCount);
                map2.put("page", 0);
                map2.put("rows", 1000);

//                List<Map<String, Object>> adSumList = adService.selectAd(map);
//                List<Map<String, Object>> adSumList = adService.selectItemData(map);
                List<Map<String, Object>> adSumList = itemsService.selectItemData(map);
                System.out.println("+++adSumList："+adSumList);
                System.out.println("+++map："+map);

                // 表格全部页汇总
                totalRow = adService.selectAllSum(adSumList, map);

    //            adList.add(adSumMap);

            }



            // 曲线图
            Map lineMaps = new HashMap();
            List<Map<String, Object>> lineList = adService.selectAdItems(map);
            List legendData = new ArrayList();
            List seriesData = new ArrayList();
            List xAxisData = new ArrayList();

            int adSize = AdEnum.values().length;
            for(int i = 1; i <= adSize; i++){
                String keys = AdEnum.getKeys(i);
                String values = AdEnum.getValues(i);


                legendData.add(values);


                List seriesList = new ArrayList();
                for(Map<String, Object> lineMap : lineList){

                    if(keys.equals("operateCost")){ // 计算运营成本

                        keys = keys + "Ratio";
                        if(isKey(keys, lineMap)){
                            BigDecimal revenue = new BigDecimal(String.valueOf(lineMap.get("revenue")));
                            BigDecimal operateCostRatio = new BigDecimal(String.valueOf(lineMap.getOrDefault("operateCostRatio", 0)));
                            BigDecimal result = revenue.multiply(operateCostRatio); // 收入*运营成本占比

                            seriesList.add(result);
                        }

                    }else if(keys.equals("profit")){ // 计算利润

                        BigDecimal revenue = new BigDecimal(String.valueOf(lineMap.getOrDefault("revenue", 0))); // 收入
                        BigDecimal cost = new BigDecimal(String.valueOf(lineMap.getOrDefault("cost", 0))); // 广告成本
                        BigDecimal logisticCost = new BigDecimal(String.valueOf(lineMap.getOrDefault("logisticCost", 0))); // 物流成本
                        BigDecimal goodsCost = new BigDecimal(String.valueOf(lineMap.getOrDefault("goodsCost", 0))); // 商品成本
                        BigDecimal operateCostRatio = new BigDecimal(String.valueOf(lineMap.getOrDefault("operateCostRatio", 0))); // 运营成本占比
                        BigDecimal refund = new BigDecimal(String.valueOf(lineMap.getOrDefault("refund", 0))); // 退款
                        BigDecimal toolCost = new BigDecimal(String.valueOf(lineMap.getOrDefault("toolCost", 0))); // 工具成本
                        BigDecimal passCost = new BigDecimal(String.valueOf(lineMap.getOrDefault("passCost", 0))); // 通道成本
                        // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
                        BigDecimal operateCost = revenue.multiply(operateCostRatio);
                        BigDecimal result = revenue.subtract(cost).subtract(logisticCost).subtract(goodsCost).subtract(refund).subtract(toolCost).subtract(passCost).subtract(operateCost);
                        seriesList.add(result);


                    }else if(isKey(keys, lineMap)){
                        BigDecimal result = new BigDecimal(String.valueOf(lineMap.get(keys)));
                        seriesList.add(result);
                    }

                }
                Map seriesMap = new HashMap();
                seriesMap.put("data", seriesList);
                seriesMap.put("name", values);
                seriesMap.put("type", "line");
                seriesData.add(seriesMap);

            }

            for(Map<String, Object> lineMap : lineList){
                Date createTime = (Date)lineMap.get("create_time");
                xAxisData.add(DateUtil.formatDate(createTime, "MM月dd日"));

            }

            lineMaps.put("legendData", legendData);
            lineMaps.put("seriesData", seriesData);
            lineMaps.put("xAxisData", xAxisData);


            JsonResult data = JsonResult.ok(0, adCount,"成功").put("data", adList).put("lineData", lineMaps).put("totalRow", totalRow);

            System.out.println("站点数据 data:"+JSONObject.toJSON(data));
            return data;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return JsonResult.error("添加失败");
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
    @OperLog(value = "站点数据", desc = "添加", result = true)
    @RequiresPermissions("sys:itemsData:add")
    @ResponseBody
    @RequestMapping("/add")
    public JsonResult add(HttpServletRequest request, @RequestParam(name = "itemsIdSearch", required = false)Long itemsId
            , @RequestParam(name = "logisticCost", required = false)String logisticCost, @RequestParam(name = "logisticCostRatio", required = false)String logisticCostRatio
            , @RequestParam(name = "goodsCost", required = false)String goodsCost, @RequestParam(name = "goodsCostRatio", required = false)String goodsCostRatio
            , @RequestParam(name = "operateCostRatio", required = false)String operateCostRatio
            , @RequestParam(name = "refund", required = false)String refund, @RequestParam(name = "refundRate", required = false)String refundRate
            , @RequestParam(name = "toolCost", required = false)String toolCost, @RequestParam(name = "toolCostRatio", required = false)String toolCostRatio
            , @RequestParam(name = "passCost", required = false)String passCost, @RequestParam(name = "passCostRatio", required = false)String passCostRatio) {

        System.out.println("站点数据添加 add:"+request);

        System.out.println("itemsId:"+itemsId);
        System.out.println("logisticCost:"+logisticCost);

        String logisticCost2 = request.getParameter("logisticCost");
        System.out.println("logisticCost2:"+logisticCost2);

        try {
            Map map = new HashMap();
            map.put("items_id", itemsId);
            map.put("logisticCost", logisticCost);
            map.put("logisticCostRatio", logisticCostRatio);
            map.put("goodsCost", goodsCost);
            map.put("goodsCostRatio", goodsCostRatio);
            map.put("operateCostRatio", operateCostRatio);
            map.put("refund", refund);
            map.put("refundRate", refundRate);
            map.put("toolCost", toolCost);
            map.put("toolCostRatio", toolCostRatio);
            map.put("passCost", passCost);
            map.put("passCostRatio", passCostRatio);
            System.out.println("map："+map);

            List<Map<String, Object>> adsList = adService.selectAdsByItemsId(itemsId);

            if(adsList.size() > 0){
                System.out.println("修改成功了");
                adService.updateAds(map);
            }else{
                System.out.println("添加成功了");
                adService.insertAds(map);
            }
            return JsonResult.ok("添加成功");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return JsonResult.error("添加失败");
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
    @RequiresPermissions("sys:itemsData:list")
    @ResponseBody
    @RequestMapping("/getSite")
    public JsonResult getSite(HttpServletRequest request) {

        System.out.println("++++++++++获取站点 list："+request);

/*
        Map data = new HashMap();
        data.put("code", 200);
        data.put("data", siteStr);
        data.put("msg", "操作成功");
        System.out.println("data："+data);
*/


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


        Map map = new HashMap();
        map.put("page", 0);
        map.put("jobNumber", loginJobNumber);
        map.put("rows", 100);


        System.out.println("getSite map："+map);
        List<Map<String, Object>> itemsList = null;
        if(isEmployee){
            itemsList = itemsService.selectItemsByJobNumber(map);
        }else{
            itemsList = itemsService.selectItems(map);
        }

//        List<Map<String, Object>> gaList = itemsService.selectItemsByType(0);
//        List<Map<String, Object>> fbList = itemsService.selectItemsByType(1);



        JsonResult data = JsonResult.ok().put("data", JSONObject.toJSONString(itemsList));
        System.out.println("getSite data："+JSONObject.toJSON(data));
        return data;

    }





/*
    @ApiOperation(value = "获取工号")
    @PreAuthorize("hasAuthority('get:/reporting/getJob')")
    @GetMapping("/getJob")
*/


    /**
     * 获取工号
     */
    @OperLog(value = "站点数据", desc = "获取工号", param = false, result = true)
    @RequiresPermissions("sys:itemsData:list")
    @ResponseBody
    @RequestMapping("/getJob")
    public JsonResult getJob(HttpServletRequest request) {



        User loginUser = getLoginUser();
        String loginJobNumber = loginUser.getJobNumber();
        List<Role> rolesList = loginUser.getRoles();
        boolean isEmployee = true; // 是否员工
        for(Role  role : rolesList){
            if(role.getRoleId() < 3){
                isEmployee = false;
                loginJobNumber = null;
                break;
            }
        }



        System.out.println("+++++++++获取工号");
        String itemsId = request.getParameter("itemsId");
        System.out.println("itemsId:"+itemsId);
        System.out.println("loginJobNumber:"+loginJobNumber);

//        List<Map<String, Object>> itemsList = jobNumberService.selectJobNumberByItemsId(itemsId);
        List<Map<String, Object>> itemsList = adService.selectAdGroupByJobNumber(Long.valueOf(itemsId), loginJobNumber);
        System.out.println("itemsList:"+itemsList);

//        String data2 = AnalyticsUtil.getData("206036759", "ostudio01@ostudio01.iam.gserviceaccount.com", "ostudio01-788809f30767.p12");
//        System.out.println("data2："+data2);


        JsonResult data = JsonResult.ok().put("data", itemsList);

        return data;
    }



/*
    @ApiOperation(value = "获取广告账户")
    @PreAuthorize("hasAuthority('get:/reporting/getAdAccount')")
    @GetMapping("/getAdAccount")
*/

    /**
     * 获取广告账户
     */
    @OperLog(value = "站点数据", desc = "获取广告账户", param = false, result = true)
    @RequiresPermissions("sys:itemsData:list")
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

    // 同步Google数据
    public boolean syncGoogle(long time, int distanceOfTwoDate){

        try {
            Map maps = new HashMap();
            // 查询证书
            List<Map<String, Object>> certificateList = certificateService.selectAllCertificate(maps);
            System.out.println("certificateList："+certificateList);

            Map<String, Object> certificateMap = certificateList.get(0);
            String serviceAccountId = (String)certificateMap.get("service_account_id");
            String path = (String)certificateMap.get("path");

            File orgFile = new File(File.listRoots()[UploadConstant.UPLOAD_DIS_INDEX], UploadConstant.UPLOAD_DIR + path);

            System.out.println("serviceAccountId："+serviceAccountId);
            System.out.println("orgFile.getPath()："+orgFile.getPath());
            List<AccountSummary> itemList = AnalyticsUtil.getItems(serviceAccountId, orgFile.getPath());

            System.out.println("itemList:"+itemList);

//        Long time = startDate.getTime();
            if(certificateList.size() > 0){

                for(int j = 0; j <= distanceOfTwoDate; j++){

                    String yesterdayDate = DateUtil.timestampToTime(time, "yyyy-MM-dd");

                    // 先删除数据再重新记录
                    Map adMap = new HashMap();
                    adMap.put("type", Constants.GA_AD);
                    adMap.put("create_time", yesterdayDate);
                    adService.deleteByType(adMap);

                    for(AccountSummary item : itemList){
                        String id = item.getId();
                        String name = item.getName();
                        List<WebPropertySummary> webPropertiesList = item.getWebProperties();
                        for(WebPropertySummary WebProperty : webPropertiesList){
                            List<ProfileSummary> profiles = WebProperty.getProfiles();
                            for(ProfileSummary profile : profiles){
                                String profileId = (String)profile.get("id");

                                try {

                                    //            String adData = AnalyticsUtil.getAdData("206036759", "ostudio01@ostudio01.iam.gserviceaccount.com"
                                    //                    , "ostudio01-788809f30767.p12", yesterdayDate, yesterdayDate);
                                    System.out.println("..............profileId:"+profileId);
                                    // 调谷歌接口获取数据
                                    String adData = AnalyticsUtil.getAdData(String.valueOf(profileId), serviceAccountId
                                            , orgFile.getPath(), yesterdayDate, yesterdayDate);
                                    System.out.println("--------------adData："+adData);

                                    //                            logger.warning("adTasks adData：" + adData);

                                    JSONObject adObj = JSONObject.parseObject(adData);
                                    JSONArray rowsArr = adObj.getJSONArray("rows");

                                    JSONObject profileInfoObj = adObj.getJSONObject("profileInfo");
                                    Long accountId = profileInfoObj.getLong("accountId");

                                    if(rowsArr != null && rowsArr.size() > 0){
                                        Map map = new HashMap<>();
                                        for(int i = 0; i < rowsArr.size(); i++){
                                            JSONArray adArr = rowsArr.getJSONArray(i);
                                            String adName = adArr.getString(0); // 广告名称
                                            String adAccount = adArr.getString(1); // 广告账户ID
                                            String source = adArr.getString(2); // 广告渠道
                                            String revenue = adArr.getString(3); // 收入
                                            String cost = adArr.getString(4); // 广告成本

                                            map.put("items_id", accountId);
                                            map.put("profiles_id", profileId);

                                            String jobNumber = "";
                                            if(adName.contains("[")){
                                                // 截取广告名称中的工号
                                                String result2 = adName.substring(0, adName.indexOf("["));
                                                jobNumber = adName.substring(result2.length()+1, adName.length()-1);
                                            }

                                            map.put("job_number", jobNumber);
                                            map.put("ad_account", adAccount);
                                            //                                map.put("ad_name", adName); // 有特殊字符，会报错
                                            map.put("ad_name", "");
                                            map.put("source", source);
                                            map.put("revenue", revenue);
                                            map.put("cost", cost);
                                            map.put("type", 0);
                                            map.put("create_time", DateUtil.timestampToTime(time, "yyyy-MM-dd"));

                                            System.out.println("-----------------------map:"+map);

                                            adService.insertAd(map);
                                        }
                                    }

                                } catch (Exception e) {
                                    e.printStackTrace();

                                }
                            }
                        }
                    }

                    time += 86400000;
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }


    // 同步Facebook数据
    public boolean syncFacebook(long time, int distanceOfTwoDate){

        System.out.println("同步Facebook数据："+time);

        try {
            // 获取广告账户
            MultiValueMap<String, String> adAccountParams = new LinkedMultiValueMap<>();
            adAccountParams.add("access_token", FBConstants.ACCESS_TOKEN);
            adAccountParams.add("fields", "id,name,account_id,spend");

            String adAccountUrl = FBConstants.GRAPH_URL + FBConstants.BUSINESS_ID + "/client_ad_accounts?";
//        logger.info("adAccountParams："+adAccountParams);
            System.out.println("adAccountParams："+adAccountParams);

//            String fields = "id,name,account_id,spend";
//            adAccountUrl = adAccountUrl + "access_token=" + ACCESS_TOKEN + "&fields=" + fields;

//            logger.info("adAccountUrl："+adAccountUrl);
            System.out.println("adAccountUrl："+adAccountUrl);


            Map adAccountMap = new HashMap();
            adAccountMap.put("access_token", FBConstants.ACCESS_TOKEN);
            adAccountMap.put("fields", "id,name,account_id,spend,adcreatives{id,name,url_tags}");

            HttpUtil httpUtil = new HttpUtil();

            // 获取广告账户数据
            String adAccountResult = httpUtil.doGet(adAccountUrl, adAccountMap);
//            System.out.println("adAccountResult:"+adAccountResult);

            JSONObject adAccountObject = JSONObject.parseObject(adAccountResult);
            System.out.println("adAccountObject："+adAccountObject);
            JSONArray adAccountDataArr = adAccountObject.getJSONArray("data");

            for(int j = 0; j <= distanceOfTwoDate; j++){

                // 删除数据库当天的广告
                String timeRange = DateUtil.timestampToTime(time, "yyyy-MM-dd");
                Map adMap = new HashMap();
                adMap.put("create_time", timeRange);
                adMap.put("type", Constants.FB_AD);
                adService.deleteByType(adMap);

                // 遍历广告账户
                for(int i = 0; i < adAccountDataArr.size(); i++) {
                    JSONObject adAccountObj = adAccountDataArr.getJSONObject(i);
                    String adAccountId = adAccountObj.getString("account_id");
                    JSONObject adCreativesObj = adAccountObj.getJSONObject("adcreatives");
                    String adCreativesStr = adCreativesObj.toJSONString();

                    // 如果没有工号就跳过
                    if (!adCreativesStr.contains("%5B") && !adCreativesStr.contains("%5D")) {
                        continue;
                    }

                    // 截取工号
                    String start = adCreativesStr.substring(0, adCreativesStr.indexOf("%5B"));
                    String end = adCreativesStr.substring(0, adCreativesStr.indexOf("%5D"));
                    String jobNumber = adCreativesStr.substring(start.length() + 3, end.length());

                    // 获取广告详情URL
                    String insightsUrl = FBConstants.GRAPH_URL + "act_" + adAccountId + "/insights?";

                    System.out.println("campaignsUrl:" + insightsUrl);

                    String insightsFields = "account_id,spend,ad_id,campaign_id,date_start,date_stop,account_name,website_purchase_roas";
                    Map insightsMap = new HashMap();
                    insightsMap.put("access_token", FBConstants.ACCESS_TOKEN);
                    //            campaignsParams.put("time_range", "%7b'since':'2020-08-31','until':'2020-08-31'%7d");
                    insightsMap.put("time_range", "{'since':'"+timeRange+"','until':'"+timeRange+"'}");
                    insightsMap.put("fields", insightsFields);

                    //            String campaignsResult = HttpUtil.get(campaignsUrl, campaignsParams);
                    String insightsResult = httpUtil.doGet(insightsUrl, insightsMap);
                    System.out.println(".............campaignsResult：" + insightsResult);

                    JSONObject insightsObjs = JSONObject.parseObject(insightsResult);
                    JSONArray insightsDataArr = insightsObjs.getJSONArray("data");
                    System.out.println("insightsDataArr："+insightsDataArr + " i："+i);

                    if (insightsDataArr.size() > 0) {

                        JSONObject insightsObj = insightsDataArr.getJSONObject(0);
                        Double spend = insightsObj.getDouble("spend"); // 成本
                        String date = insightsObj.getString("date_start");
                        String accountName = insightsObj.getString("account_name");

//                        JSONArray purchaseRoasArr = insightsObj.getJSONArray("purchase_roas"); // 花费回报
                        JSONArray purchaseRoasArr = insightsObj.getJSONArray("website_purchase_roas"); // 花费回报
                        Double value = 0.00;
                        if (purchaseRoasArr != null) {
                            JSONObject purchaseRoasObj = purchaseRoasArr.getJSONObject(0);
                            value = purchaseRoasObj.getDouble("value");
                        }

                        Map dataMap = new HashMap<>();
                        dataMap.put("items_id", adAccountId);
                        //            map.put("job_number", jobNumber);
                        dataMap.put("job_number", jobNumber);
                        dataMap.put("ad_account", adAccountId);
                        //            map.put("ad_name", campaignsName);
                        dataMap.put("ad_name", accountName);
                        dataMap.put("source", "facebook.com/cpc"); // 固定不变 写死

                        BigDecimal revenue = new BigDecimal(spend * value).setScale(2, RoundingMode.HALF_UP);
                        dataMap.put("revenue", String.format("%.2f", revenue)); // 收入
                        dataMap.put("cost", spend); // 成本
                        dataMap.put("type", 1); // 成本 ga0 fb1
                        dataMap.put("create_time", date);

                        System.out.println("++++++++++++++++map:" + dataMap);

                        adService.insertAd(dataMap);

                    }
                }
                // 每次循环加一天
                time += 86400000;
            }


            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;

    }


    /**
     * 添加数据
     */
    @OperLog(value = "站点数据", desc = "同步数据", result = true)
//    @RequiresPermissions("sys:itemsData:add")
    @ResponseBody
    @RequestMapping("/syncData")
    public JsonResult syncData(HttpServletRequest request, @RequestParam(name = "platform", required = false)Integer platform
            , @RequestParam(name = "dateRange", required = false)String dateRange) {

        System.out.println("站点数据同步 开始：" + DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH;mm:ss:SSS"));

        try {
            String startTime = StringUtils.substringBefore(dateRange, " - ");
            String endTime = StringUtils.substringAfter(dateRange, " - ");
            if(StringUtils.isEmpty(startTime) || StringUtils.isEmpty(endTime)){
                return JsonResult.error("日期不能为空");
            }


            int distanceOfTwoDate = DateUtil.getDistanceOfTwoDate(startTime, endTime); // 3
            System.out.println("distanceOfTwoDate："+distanceOfTwoDate);

            if(distanceOfTwoDate > 30){
                return JsonResult.error("日期范围不能大于30天");
            }

            Date startDate = DateUtil.parseDate(startTime, "yyyy-MM-dd");
            Date endDate = DateUtil.parseDate(endTime, "yyyy-MM-dd");
            if(startDate.getTime() > endDate.getTime()){
                return JsonResult.error("日期错误");
            }
/*

            if(true){
                Thread.sleep(6000);
                System.out.println("站点数据同步 结束：" + DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH;mm:ss:SSS"));
                return JsonResult.ok("添加成功");
            }
*/


            boolean result = false;
            if(platform == 0){
                System.out.println("开始同步google");
                result = syncGoogle(startDate.getTime(), distanceOfTwoDate);
            }else{
                System.out.println("开始同步facebook");
                result = syncFacebook(startDate.getTime(), distanceOfTwoDate);
            }



            System.out.println("站点数据同步 结束：" + DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH;mm:ss:SSS"));

            if(result){
                return JsonResult.ok("同步成功");
            }else{
                return JsonResult.error("同步失败");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return JsonResult.error("同步失败");
    }

}
