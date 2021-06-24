package com.egao.common.system.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.Cache;
import com.egao.common.core.Constants;
import com.egao.common.core.FBConstants;
import com.egao.common.core.UploadConstant;
import com.egao.common.core.annotation.OperLog;
import com.egao.common.core.utils.*;
import com.egao.common.core.web.BaseController;
import com.egao.common.core.web.JsonResult;
import com.egao.common.core.web.PageParam;
import com.egao.common.system.entity.Role;
import com.egao.common.system.entity.User;
import com.egao.common.system.mapper.ItemsMapper;
import com.egao.common.system.service.*;
import com.google.api.services.analytics.model.AccountSummary;
import com.google.api.services.analytics.model.ProfileSummary;
import com.google.api.services.analytics.model.WebPropertySummary;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.formula.functions.T;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.apache.tika.utils.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;

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
@RequestMapping("/sys/itemsData")
public class ItemsDataController extends BaseController {

//    @Autowired
//    private LZGameService gameService;


    public static String GRAPH_URL = "https://graph.facebook.com/v7.0/";

    public static String ACCESS_TOKEN = "EAAH92JtasVMBAJ2iHbMXEdLwzMZAH2PidkMGwvQbhFZCZAAcPmUHOxfwaPfNg4M3vXCBonOVZAHLIrj7gdZCJqT9pQs8CAMGrBp7ECuNKOdFIO5txnP3UylNAI959oXBqp1hZAJloEBqSvVdt3hVhXYDu7WGdoZCgZCqrqX0PVE5LKKdGtlzQMxZBmrY8YWjQARUZD";

    public static String BUSINESS_ID = "144436283227029";


    @Autowired
    private AdService adService;

    @Autowired
    private ItemsService itemsService;

    @Autowired
    private CertificateService certificateService;

    @Autowired
    private ChannelService channelService;

    @Autowired
    private BusinessService businessService;

    @Autowired
    public ItemsMapper itemsMapper;

    @Autowired
    public UserItemService userItemService;


    @RequiresPermissions("sys:itemsData:view")
    @RequestMapping()
    public String view(Model model) {

        System.out.println("加载页面itemsData.html");

        List<Map<String, Object>> channelList = channelService.selectChannelList(new HashMap());

        Map map = new HashMap();
        map.put("value", "0");
        map.put("name", "其它");
        channelList.add(map);

        model.addAttribute("channelList", JSON.toJSONString(channelList));
//        model.addAttribute("groupStatus", groupStatus);

//        String dataStr = "[{\"channelId\":294,\"channelName\":\"test23\",\"cost\":2219.49,\"costProportion\":26.24,\"goodsCost\":1691.67,\"goodsCostRatio\":20.00,\"itemsId\":145680583,\"itemsName\":\"Inspireyoos\",\"job_number\":\"\",\"logisticCost\":1268.75,\"logisticCostRatio\":15.00,\"month\":\"2020-10\",\"operateCost\":676.67,\"operateCostRatio\":8.00,\"passCost\":422.92,\"passCostRatio\":5.00,\"profit\":1840.51,\"profitRate\":21.76,\"refund\":211.46,\"refundRate\":2.50,\"revenue\":8458.34,\"roas\":3.81,\"toolCost\":126.88,\"toolCostRatio\":1.50},{\"channelId\":294,\"channelName\":\"test23\",\"cost\":11.72,\"costProportion\":0.36,\"goodsCost\":15.26,\"goodsCostRatio\":0.47,\"itemsId\":134723789,\"itemsName\":\"Sonsoulier\",\"job_number\":\"\",\"logisticCost\":11.45,\"logisticCostRatio\":0.35,\"month\":\"2020-10\",\"operateCost\":20.03,\"operateCostRatio\":0.62,\"passCost\":5.72,\"passCostRatio\":0.18,\"profit\":3116.27,\"profitRate\":96.33,\"refund\":24.80,\"refundRate\":0.77,\"revenue\":3234.83,\"roas\":276.01,\"toolCost\":29.57,\"toolCostRatio\":0.91},{\"channelId\":294,\"channelName\":\"test23\",\"cost\":318.02,\"costProportion\":13.22,\"goodsCost\":481.16,\"goodsCostRatio\":20.00,\"itemsId\":152409799,\"itemsName\":\"Vooglabor\",\"job_number\":\"\",\"logisticCost\":360.87,\"logisticCostRatio\":15.00,\"month\":\"2020-10\",\"operateCost\":192.46,\"operateCostRatio\":8.00,\"passCost\":120.29,\"passCostRatio\":5.00,\"profit\":836.75,\"profitRate\":34.78,\"refund\":60.14,\"refundRate\":2.50,\"revenue\":2405.78,\"roas\":7.56,\"toolCost\":36.09,\"toolCostRatio\":1.50},{\"channelId\":294,\"channelName\":\"test23\",\"cost\":362.24,\"costProportion\":28.27,\"goodsCost\":13.50,\"goodsCostRatio\":1.05,\"itemsId\":118674272,\"itemsName\":\"Obangbag\",\"job_number\":\"\",\"logisticCost\":13.50,\"logisticCostRatio\":1.05,\"month\":\"2020-10\",\"operateCost\":20.25,\"operateCostRatio\":1.58,\"passCost\":40.50,\"passCostRatio\":3.16,\"profit\":784.05,\"profitRate\":61.19,\"refund\":20.25,\"refundRate\":1.58,\"revenue\":1281.29,\"roas\":3.54,\"toolCost\":27.00,\"toolCostRatio\":2.11},{\"channelId\":294,\"channelName\":\"test23\",\"cost\":0.00,\"costProportion\":0.00,\"goodsCost\":205.53,\"goodsCostRatio\":20.00,\"itemsId\":137727597,\"itemsName\":\"Vaschoen\",\"job_number\":\"\",\"logisticCost\":154.15,\"logisticCostRatio\":15.00,\"month\":\"2020-10\",\"operateCost\":82.21,\"operateCostRatio\":8.00,\"passCost\":51.38,\"passCostRatio\":5.00,\"profit\":493.27,\"profitRate\":48.00,\"refund\":25.69,\"refundRate\":2.50,\"revenue\":1027.64,\"roas\":0.00,\"toolCost\":15.41,\"toolCostRatio\":1.50},{\"channelId\":294,\"channelName\":\"test23\",\"cost\":0.98,\"costProportion\":0.12,\"goodsCost\":156.94,\"goodsCostRatio\":20.00,\"itemsId\":164589470,\"itemsName\":\"Eugenstern\",\"job_number\":\"\",\"logisticCost\":117.71,\"logisticCostRatio\":15.00,\"month\":\"2020-10\",\"operateCost\":62.78,\"operateCostRatio\":8.00,\"passCost\":39.24,\"passCostRatio\":5.00,\"profit\":375.68,\"profitRate\":47.88,\"refund\":19.62,\"refundRate\":2.50,\"revenue\":784.71,\"roas\":800.72,\"toolCost\":11.77,\"toolCostRatio\":1.50},{\"channelId\":294,\"channelName\":\"test23\",\"cost\":0.00,\"costProportion\":0.00,\"goodsCost\":139.71,\"goodsCostRatio\":20.00,\"itemsId\":145434736,\"itemsName\":\"beejour\",\"job_number\":\"\",\"logisticCost\":104.78,\"logisticCostRatio\":15.00,\"month\":\"2020-10\",\"operateCost\":55.88,\"operateCostRatio\":8.00,\"passCost\":34.93,\"passCostRatio\":5.00,\"profit\":335.29,\"profitRate\":48.00,\"refund\":17.46,\"refundRate\":2.50,\"revenue\":698.53,\"roas\":0.00,\"toolCost\":10.48,\"toolCostRatio\":1.50},{\"channelId\":294,\"channelName\":\"test23\",\"cost\":0.00,\"costProportion\":0.00,\"goodsCost\":51.36,\"goodsCostRatio\":20.00,\"itemsId\":152395144,\"itemsName\":\"Choco-bon\",\"job_number\":\"\",\"logisticCost\":38.52,\"logisticCostRatio\":15.00,\"month\":\"2020-10\",\"operateCost\":20.55,\"operateCostRatio\":8.00,\"passCost\":12.84,\"passCostRatio\":5.00,\"profit\":123.27,\"profitRate\":48.00,\"refund\":6.42,\"refundRate\":2.50,\"revenue\":256.82,\"roas\":0.00,\"toolCost\":3.85,\"toolCostRatio\":1.50},{\"channelId\":294,\"channelName\":\"test23\",\"cost\":0.01,\"costProportion\":0.01,\"goodsCost\":26.94,\"goodsCostRatio\":20.00,\"itemsId\":164611059,\"itemsName\":\"Doraym\",\"job_number\":\"\",\"logisticCost\":20.21,\"logisticCostRatio\":15.00,\"month\":\"2020-10\",\"operateCost\":10.78,\"operateCostRatio\":8.00,\"passCost\":6.74,\"passCostRatio\":5.00,\"profit\":64.66,\"profitRate\":48.00,\"refund\":3.37,\"refundRate\":2.50,\"revenue\":134.72,\"roas\":13472.00,\"toolCost\":2.02,\"toolCostRatio\":1.50},{\"channelId\":294,\"channelName\":\"test23\",\"cost\":0.00,\"costProportion\":0.00,\"goodsCost\":8.40,\"goodsCostRatio\":20.00,\"itemsId\":142601258,\"itemsName\":\"Yoosbuys\",\"job_number\":\"\",\"logisticCost\":6.30,\"logisticCostRatio\":15.00,\"month\":\"2020-10\",\"operateCost\":3.36,\"operateCostRatio\":8.00,\"passCost\":2.10,\"passCostRatio\":5.00,\"profit\":20.16,\"profitRate\":48.00,\"refund\":1.05,\"refundRate\":2.50,\"revenue\":42.00,\"roas\":0.00,\"toolCost\":0.63,\"toolCostRatio\":1.50}]";
//        model.addAttribute("dataList", dataStr);


        return "system/itemsData.html";
    }



    @OperLog(value = "站点数据", desc = "分页查询")
    @RequiresPermissions("sys:itemsData:list")
    @ResponseBody
    @RequestMapping("/page")
    public JsonResult page(HttpServletRequest request
            , @RequestParam(name = "page", required = false, defaultValue = "1")Integer page, @RequestParam(name = "limit", required = false, defaultValue = "20")Integer limit
            , @RequestParam(name = "itemsId", required = false)String itemsId, @RequestParam(name = "jobNumber", required = false, defaultValue = "")String jobNumber
            , @RequestParam(name = "adAccount", required = false, defaultValue = "")String adAccount, @RequestParam(name = "searchTime", required = false)String searchTime
            , @RequestParam(name = "adChannel", required = false)String adChannel, @RequestParam(name = "channelId", required = false)String channelId) {

        try {

            System.out.println("站点数据 分页查询数据...");

            System.out.println("page："+(page-1)*limit);
            System.out.println("limit："+limit);
            System.out.println("searchTime："+searchTime);


/*
            String selectTypeStr = request.getParameter("selectType");
            Integer selectType =  StringUtils.isEmpty(selectTypeStr) ? 0 : Integer.valueOf(selectTypeStr);

            System.out.println("selectType："+selectType);
            System.out.println("groupStatus："+groupStatus);

            // selectType 0查询 1渠道分组
            // groupStatus 0不分组 1分组
            if(selectType == 1){
                groupStatus = groupStatus == 0 ? 1 : 0;
            }

            System.out.println("groupStatus2："+groupStatus);
*/


            User user = getLoginUser();
            Integer userId = user.getUserId();

            searchTime = searchTime == null ? "" : searchTime;
            String loginJobNumber = user.getJobNumber();
            List<Role> rolesList = user.getRoles();

            boolean isEmployee = false; // 是否员工

/*
            boolean isEmployee = true; // 是否员工
            for(Role  role : rolesList){
                if(role.getRoleId() < 3){
                    isEmployee = false;
                    break;
                }
            }
*/


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


//            limit = 20;
            Map map = new HashMap();
            map.put("page", (page-1)*limit);
            map.put("rows", limit);
//        map.put("rows", 100);
            map.put("itemsId", itemsId == null ? "" : itemsId);
//            jobNumber = "无工号".equals(jobNumber) ? "000" : jobNumber;
                map.put("jobNumber", isEmployee ? loginJobNumber : jobNumber);
            map.put("adAccount", adAccount == null ? "" : adAccount);
            map.put("adChannel", adChannel == null ? "" : adChannel);
            map.put("startTime", startTime);
            map.put("endTime", endTime);
            map.put("channelId", channelId);
            map.put("groupStatus", user.getGroupStatus());
            map.put("userId", userId);


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
            map2.put("userId", userId);

            System.out.println("站点数据 userId:"+userId);
            System.out.println("站点数据 map:"+map);

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
                    String job_number = (String)adMap.get("job_number");
                    String channelName = (String)adMap.get("channelName");

                    // 根据itemsId获取缓存中的itemsName
                    if(itemsName == null){
                        String itemsNameCache = Cache.getItemsName(String.valueOf(itemId));
                        adMap.put("itemsName", itemsNameCache);
                    }

                    // 冗余代码
                    adMap.put("job_number", StringUtils.isEmpty(jobNumber) ? "" : job_number);

                    if(StringUtils.isEmpty(channelName)){
                        adMap.put("channelId", 0);
                        adMap.put("channelName", "其他");

                    }

/*
                    List<Map<String, Object>> userItemList = userItemService.selectUserItemByUserId(userId);
                    for(Map<String, Object> userItemMap : userItemList){
                        Integer itemsId2 = (Integer)userItemMap.get("itemsId");
                        if(itemsId.equals(Long.valueOf(itemId))){
                            itemsMap.put("checked", true);
                            break;
                        }
                    }
*/





                }


//                adCount = adService.selectAdCount(map);
//                System.out.println("-------adCount："+adCount);
                map2.put("page", 0);
                map2.put("rows", 1000);


//                List<Map<String, Object>> adSumList = adService.selectAd(map);
//                List<Map<String, Object>> adSumList = adService.selectItemData(map);
//                List<Map<String, Object>> adSumList = itemsService.selectItemData(map);
//                System.out.println("+++adSumList："+adSumList);
                System.out.println("+++map："+map);

                // 表格全部页汇总
                totalRow = adService.selectAllSum(adList, map);

    //            adList.add(adSumMap);

            }



            // 曲线图
            Map lineMaps = new HashMap();
//            List<Map<String, Object>> lineList = adService.selectAdItems(map);
            List legendData = new ArrayList();
            List seriesData = new ArrayList();
            List xAxisData = new ArrayList();

            List<Map<String, Object>> chartList = itemsService.selectChartData(map);
            System.out.println("chartListJSON："+JSONArray.toJSONString(chartList));


            int adSize = AdEnum.values().length;
            for(int i = 1; i <= adSize; i++){
                String keys = AdEnum.getKeys(i);
                String values = AdEnum.getValues(i);

                legendData.add(values);

                List seriesList = new ArrayList();
//                for(Map<String, Object> lineMap : lineList){
                for(Map<String, Object> lineMap : chartList){

                    if(isKey(keys, lineMap)){
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

            System.out.println("xAxisData adList："+JSONArray.toJSONString(adList));

//            for(Map<String, Object> lineMap : lineList){
            for(Map<String, Object> lineMap : chartList){
//                Date createTime = (Date)lineMap.get("create_time");
                Date date = (Date)lineMap.get("date");
                String itemsName = (String)lineMap.get("itemsName");
                String channelName = (String)lineMap.get("channelName");
//                xAxisData.add(DateUtil.formatDate(createTime, "MM月dd日"));
                itemsName = user.getGroupStatus() == 1 ? itemsName+"/"+channelName : itemsName;
//                xAxisData.add(itemsName);
                xAxisData.add(DateUtil.formatDate(date, "yyyy-MM-dd"));

            }

            lineMaps.put("legendData", legendData);
            lineMaps.put("seriesData", seriesData);
            lineMaps.put("xAxisData", xAxisData);

            List<Map<String, Object>> data1 = new ArrayList<>();
            if(adList.size() > 0){
                data1 = new ListPageUtil(adList, page, limit).getData();
            }



            System.out.println("data data1："+JSONArray.toJSONString(data1));

//            JsonResult data = JsonResult.ok(0, adList.size(),"成功").put("data", adList).put("lineData", lineMaps).put("totalRow", totalRow);
            JsonResult data = JsonResult.ok(0, adList.size(),"成功").put("data", data1).put("lineData", lineMaps).put("totalRow", totalRow);

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



    @OperLog(value = "站点数据", desc = "分页查询")
    @RequiresPermissions("sys:itemsData:list")
    @ResponseBody
    @RequestMapping("/page2")
    public JsonResult page2(HttpServletRequest request
            , @RequestParam(name = "page", required = false, defaultValue = "1")Integer page, @RequestParam(name = "limit", required = false, defaultValue = "20")Integer limit
            , @RequestParam(name = "itemsId", required = false)String itemsId, @RequestParam(name = "jobNumber", required = false, defaultValue = "")String jobNumber
            , @RequestParam(name = "adAccount", required = false, defaultValue = "")String adAccount, @RequestParam(name = "searchTime", required = false)String searchTime
            , @RequestParam(name = "adChannel", required = false)String adChannel, @RequestParam(name = "channelId", required = false)String channelId) {


        try {
            System.out.println("站点数据 分页查询数据...");

            System.out.println("page："+(page-1)*limit);
            System.out.println("limit："+limit);
            System.out.println("searchTime："+searchTime);

            User user = getLoginUser();
            Integer groupStatus = user.getGroupStatus();


            String selectTypeStr = request.getParameter("selectType");
            Integer selectType =  StringUtils.isEmpty(selectTypeStr) ? 0 : Integer.valueOf(selectTypeStr);

            System.out.println("selectType："+selectType);
            System.out.println("groupStatus："+groupStatus);

            // selectType 0查询 1渠道分组
            // groupStatus 0不分组 1分组
            if(selectType == 1){
//                groupStatus = groupStatus == 0 ? 1 : 0;
                user.setGroupStatus(groupStatus == 0 ? 1 : 0);
            }

            System.out.println("groupStatus2："+user.getGroupStatus());


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


//            limit = 20;
            Map map = new HashMap();
            map.put("page", (page-1)*limit);
            map.put("rows", limit);
//        map.put("rows", 100);
            map.put("itemsId", itemsId == null ? "" : itemsId);
//            jobNumber = "无工号".equals(jobNumber) ? "000" : jobNumber;
            map.put("jobNumber", isEmployee ? loginJobNumber : jobNumber);
            map.put("adAccount", adAccount == null ? "" : adAccount);
            map.put("adChannel", adChannel == null ? "" : adChannel);
            map.put("startTime", startTime);
            map.put("endTime", endTime);
            map.put("channelId", channelId);
            map.put("groupStatus", user.getGroupStatus());

            if(StringUtils.isEmpty(adAccount)){
                System.out.println("adAccount是空："+adAccount);
            }else{
                System.out.println("adAccount不是空："+adAccount);
            }

            System.out.println("map："+map);
            List<Map<String, Object>> itemList = itemsMapper.selectItemData(map);
            System.out.println("站点数据 size："+itemList.size());
            System.out.println("站点数据 itemList："+JSONArray.toJSONString(itemList));
        } catch (ParseException e) {
            e.printStackTrace();
        }


        return JsonResult.error("添加失败");

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

        System.out.println("获取站点 itemsData list："+request);

        User loginUser = getLoginUser();
        String loginJobNumber = loginUser.getJobNumber();
        List<Role> rolesList = loginUser.getRoles();

        boolean isEmployee = false; // 是否员工

/*
        boolean isEmployee = true; // 是否员工
        for(Role  role : rolesList){
            if(role.getRoleId() < 3){
                isEmployee = false;
                break;
            }
        }
*/


        Integer userId = loginUser.getUserId();
        List<Map<String, Object>> userItemList = userItemService.selectUserItemByUserId(userId);



        Map map = new HashMap();
        map.put("page", 0);
        map.put("jobNumber", loginJobNumber);
        map.put("rows", 1000);


        System.out.println("getSite map："+map);
        List<Map<String, Object>> itemsList = null;
        if(isEmployee){
            itemsList = itemsService.selectItemsByJobNumber(map);
        }else{
            itemsList = itemsService.selectItems(map);
        }

        System.out.println("getSite data："+JSONObject.toJSON(userItemList));
//        JsonResult data = JsonResult.ok().put("data", JSONObject.toJSONString(itemsList));
        JsonResult data = JsonResult.ok().put("data", JSONObject.toJSONString(userItemList));

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

                for(int j = 0; j < distanceOfTwoDate; j++){

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
            logger.warning("syncGoogle error：" + e);
        }
        return false;
    }


    // 判断数据库中是否有该广告账户
    public boolean isExistAdAccount(List<Map<String, Object>> adAccountList, Long id){

        for(Map<String, Object> adAccountMap : adAccountList){
            Long listId = (Long)adAccountMap.get("value");
            if(listId.equals(id)){
                return true;
            }
        }

        return false;
    }



    // 同步Facebook数据
    public boolean syncFacebook2(){


        try {

            Map bMap = new HashMap();
            bMap.put("page", 0);
            bMap.put("rows", 1000);
            List<Map<String, Object>> businessList = businessService.selectBusiness(bMap);

            List<Map<String, Object>> adAccountAllList = itemsService.selectAllItemsByType(1);

            List adAccountList = new ArrayList();
            for(Map<String, Object> businessMap : businessList){
                System.out.println("一层for循环");
                Long businessId = (Long)businessMap.get("id");
                String businessName = (String)businessMap.get("name");

                MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
//                params.add("access_token", ACCESS_TOKEN);
//                params.add("fields", "id,name,account_id,account_status");
//                params.add("limit", "300");

//            String url = GRAPH_URL + BUSINESS_ID + "/client_ad_accounts?";
//            String url = "https://graph.facebook.com/v7.0/144436283227029/client_ad_accounts?";
                String adAccountUrl = GRAPH_URL + businessId + "/client_ad_accounts?";

                System.out.println("adAccountUrl："+adAccountUrl);


                Map adAccountMap = new HashMap();
                adAccountMap.put("access_token", ACCESS_TOKEN);
//                adAccountMap.put("fields", "id,name,account_id,spend,adcreatives{id,name,url_tags}");
//                adAccountMap.put("fields", "id,name,account_id,spend,campaigns.limit(30){id,name}");
                adAccountMap.put("fields", "id,name,account_id,spend,account_status");
                adAccountMap.put("account_status", "1");
                adAccountMap.put("limit", "300");

                HttpUtil httpUtil = new HttpUtil();

                String adAccountResult = httpUtil.doGet(adAccountUrl, adAccountMap);
                System.out.println("adAccountResult:"+adAccountResult);

                JSONObject adAccountObject = JSONObject.parseObject(adAccountResult);
                JSONArray adAccountDataArr = adAccountObject.getJSONArray("data");


                if(adAccountDataArr != null && adAccountDataArr.size() > 0){

//                    itemsService.deleteByType(1);
//                    itemsService.deleteByBusinessId(businessId, 1);


                    for(int i = 0; i < adAccountDataArr.size(); i++){

                        JSONObject adAccountObj = adAccountDataArr.getJSONObject(i);
                        Long id = adAccountObj.getLong("account_id");
                        String name = adAccountObj.getString("name");
                        String accountStatusStr = adAccountObj.getString("account_status");
                        Integer accountStatus = Integer.parseInt(accountStatusStr);
                        if(accountStatus != 1){ // 正常的广告账户状态都是1
                            continue;
                        }

                        Map map = new HashMap();
                        map.put("id", id);
                        map.put("business_id", businessId);
                        map.put("name", name);
                        map.put("type", 1);
                        System.out.println("delete map："+map);

                        boolean existAdAccount = isExistAdAccount(adAccountAllList, id);
                        if(!existAdAccount){
                            try {
                                // 先判断有没有再决定是否插入
                                itemsService.insertItems(map);
                                adAccountList.add(map);

                                System.out.println("成功 插入 map："+map);

                            } catch (Exception e) {
                                System.out.println("报错 插入 map："+map + " e："+e.getMessage());
                            }
                        }else{
                            System.out.println("存在了 id："+id+" name："+name);
                        }

                    }
                    // 添加进缓存
//                    Cache.setAdAccountList(adAccountList);
                }
                System.out.println("Cache.getAdAccountList()："+Cache.getAdAccountList());

            }
            // 添加进缓存
            Cache.setAdAccountList(adAccountList);



        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("定时任务结束时间：" + DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH;mm:ss:SSS"));

        return false;
    }



    // 同步Facebook数据
    public boolean syncFacebook(long time, int distanceOfTwoDate){

        System.out.println("同步Facebook数据 time："+time);
        System.out.println("同步Facebook数据 distanceOfTwoDate："+distanceOfTwoDate);

        try {
            // 获取广告账户

//            syncFacebook2();

            Map map = new HashMap();
            map.put("page", 0);
            map.put("rows", 1000);
            List<Map<String, Object>> businessList = businessService.selectBusiness(map);
            for(Map<String, Object> businessMap : businessList){
                System.out.println("一层for循环");
                String businessId = (String)businessMap.get("business_id");
                String businessName = (String)businessMap.get("business_name");


//            String adAccountUrl = FBConstants.GRAPH_URL + FBConstants.BUSINESS_ID + "/client_ad_accounts?";
                String adAccountUrl = FBConstants.GRAPH_URL + businessId + "/client_ad_accounts?";
//        logger.info("adAccountParams："+adAccountParams);
//            System.out.println("adAccountParams："+adAccountParams);

//            String fields = "id,name,account_id,spend";
//            adAccountUrl = adAccountUrl + "access_token=" + ACCESS_TOKEN + "&fields=" + fields;

//            logger.info("adAccountUrl："+adAccountUrl);
                System.out.println("adAccountUrl："+adAccountUrl);


                Map adAccountMap = new HashMap();
                adAccountMap.put("access_token", FBConstants.ACCESS_TOKEN);

//            adAccountMap.put("fields", "id,name,account_id,spend,adcreatives{id,name,url_tags}");
                adAccountMap.put("fields", "id,name,account_id,spend,account_status,campaigns.limit(10){name}");
//                adAccountMap.put("fields", "id,name,account_id,spend,campaigns{name}");
//                adAccountMap.put("fields", "id,name,account_id,spend");
                adAccountMap.put("account_status", "1");
                adAccountMap.put("limit", "300");

                HttpUtil httpUtil = new HttpUtil();

                // 获取广告账户数据
                String adAccountResult = httpUtil.doGet(adAccountUrl, adAccountMap);
//            System.out.println("adAccountResult:"+adAccountResult);

                JSONObject adAccountObject = JSONObject.parseObject(adAccountResult);
                System.out.println("adAccountObject："+adAccountObject);


/*
            if(true){
                return false;
            }
*/


                if(adAccountObject == null || adAccountObject.size() <= 0){
                    continue;
                }


                JSONArray adAccountDataArr = adAccountObject.getJSONArray("data");

                long tempTime = time;

                for(int j = 0; j < distanceOfTwoDate; j++){

                    System.out.println("二层for循环");

                    // 删除数据库当天的广告
                    String timeRange = DateUtil.timestampToTime(tempTime, "yyyy-MM-dd");
                    /*Map adMap = new HashMap();
                    adMap.put("create_time", timeRange);
                    adMap.put("type", Constants.FB_AD);
//                    adMap.put("items_id", businessId);

                    System.out.println("delete adMap："+adMap);
//                    adService.deleteByType(adMap);
//                    adService.deleteByItemsId(adMap);
                    adService.deleteByAdAccount(adMap);*/


                    // 遍历广告账户
                    for(int i = 0; i < adAccountDataArr.size(); i++) {
                        JSONObject adAccountObj = adAccountDataArr.getJSONObject(i);
                        String adAccountId = adAccountObj.getString("account_id");
                        String accountStatusStr = adAccountObj.getString("account_status");

                        // 删除
                        Map adMap = new HashMap();
                        adMap.put("create_time", timeRange);
                        adMap.put("type", Constants.FB_AD);
//                    adMap.put("items_id", businessId);
                        adMap.put("ad_account", adAccountId);

                        System.out.println("delete adMap："+adMap);
//                    adService.deleteByType(adMap);
//                    adService.deleteByItemsId(adMap);
                        adService.deleteByAdAccount(adMap);


                        Integer accountStatus = Integer.parseInt(accountStatusStr);
                        if(accountStatus != 1){ // 正常的广告账户状态都是1
                            continue;
                        }

                        System.out.println("adAccountObj："+adAccountObj);

                        // 从广告名称上截取工号
                        JSONObject campaigns = adAccountObj.getJSONObject("campaigns");

                        // 如果没有campaigns数组
                        if(campaigns == null || campaigns.size() <= 0){
                            continue;
                        }

                        System.out.println("campaigns："+campaigns);
                        JSONArray campaignsData = campaigns.getJSONArray("data");

                        // 如果没有广告名称
                        if(campaignsData == null || campaignsData.size() <= 0){
                            continue;
                        }
                        String campaignsDataStr = campaignsData.toJSONString();
                        campaignsDataStr = campaignsDataStr.substring(1 , campaignsDataStr.length() - 1);
                        // 如果没有工号就跳过
                        if (!campaignsDataStr.contains("[") || !campaignsDataStr.contains("]")) {
                            continue;
                        }

                        // 截取工号
                        String start = campaignsDataStr.substring(0, campaignsDataStr.indexOf("["));
                        String end = campaignsDataStr.substring(0, campaignsDataStr.indexOf("]"));
                        String jobNumber = campaignsDataStr.substring(start.length() + 1, end.length());
                        boolean isInteger = CoreUtil.isInteger(jobNumber);
                        // 如果没有广告名称
                        if(StringUtils.isEmpty(jobNumber) || jobNumber.length() < 1 || jobNumber.length() > 10 || !isInteger){
                            continue;
                        }

                        System.out.println("jobNumber："+jobNumber);




/*
                    // 从数据源中截取工号
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
*/


                        // 获取广告详情URL
                        String insightsUrl = FBConstants.GRAPH_URL + "act_" + adAccountId + "/insights?";

                        System.out.println("campaignsUrl:" + insightsUrl);

                        String insightsFields = "account_id,spend,ad_id,campaign_id,date_start,date_stop,account_name,website_purchase_roas";
                        Map insightsMap = new HashMap();
                        insightsMap.put("access_token", FBConstants.ACCESS_TOKEN);
                        //            campaignsParams.put("time_range", "%7b'since':'2020-08-31','until':'2020-08-31'%7d");
                        insightsMap.put("time_range", "{'since':'"+timeRange+"','until':'"+timeRange+"'}");
                        insightsMap.put("fields", insightsFields);

                        System.out.println("777insightsMap："+insightsMap);

                        //            String campaignsResult = HttpUtil.get(campaignsUrl, campaignsParams);
                        String insightsResult = httpUtil.doGet(insightsUrl, insightsMap);
                        System.out.println(".............campaignsResult：" + insightsResult);

                        JSONObject insightsObjs = JSONObject.parseObject(insightsResult);
                        JSONArray insightsDataArr = insightsObjs.getJSONArray("data");
                        System.out.println("insightsDataArr："+insightsDataArr + " i："+i);

                        if (insightsDataArr != null && insightsDataArr.size() > 0) {

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
//                            dataMap.put("items_id", adAccountId);
                            dataMap.put("items_id", businessId);
                            //            map.put("job_number", jobNumber);
                            dataMap.put("job_number", jobNumber);
                            dataMap.put("ad_account", adAccountId);
                            //            map.put("ad_name", campaignsName);
                            dataMap.put("ad_name", accountName);
                            dataMap.put("source", "facebook.com/cpc"); // 固定不变 写死

                            BigDecimal revenue = new BigDecimal(spend * value).setScale(2, RoundingMode.HALF_UP);
//                        dataMap.put("revenue", String.format("%.2f", revenue)); // FB不抓取收入
                            dataMap.put("revenue", new BigDecimal(0.00)); // 收入
                            dataMap.put("cost", spend); // 成本
                            dataMap.put("type", 1); // 成本 ga0 fb1
                            dataMap.put("create_time", date);

                            System.out.println("+++++++++++++++++");
                            System.out.println("delete map:" + dataMap);

                            adService.insertAd(dataMap);

                        }
                    }
                    // 每次循环加一天
                    tempTime += 86400000;
                    System.out.println("777777777777777777循环完了："+j);
                }
            }

            return true;
        } catch (Exception e) {
            e.printStackTrace();
            logger.warning("syncFacebook error：" + e);

        }
        return false;

    }


    public void syncGoogleItem(){

        try {
            System.out.println("syncGoogleItem startTime:"+System.currentTimeMillis());

            Map maps = new HashMap();
            List<Map<String, Object>> certificateList = certificateService.selectAllCertificate(maps);


            System.out.println("certificateList:"+certificateList);
            if(certificateList.size() > 0){
                Map<String, Object> certificateMap = certificateList.get(0);
                String serviceAccountId = (String)certificateMap.get("service_account_id");
                String path = (String)certificateMap.get("path");

                File orgFile = new File(File.listRoots()[UploadConstant.UPLOAD_DIS_INDEX], UploadConstant.UPLOAD_DIR + path);

                System.out.println("orgFile："+orgFile);

                List<AccountSummary> itemList = AnalyticsUtil.getItems(serviceAccountId, orgFile.getPath());

                System.out.println("itemsTasks itemList:" + JSONArray.toJSONString(itemList));

                if(itemList != null && itemList.size() > 0){
                    itemsService.deleteByType(0);
                    List itemsList = new ArrayList();
                    for(AccountSummary item : itemList){
                        String id = item.getId();
                        String name = item.getName();

                        Map map = new HashMap();
                        map.put("id", id);
                        map.put("business_id", null);
                        map.put("name", name);
                        map.put("type", 0);

                        itemsList.add(map);

                        itemsService.insertItems(map);
                    }
                    // 添加进缓存
                    Cache.setItemsList(itemsList);
                }
                System.out.println("Cache.getItemsList()："+Cache.getItemsList());

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    /**
     * 同步数据
     */
    @OperLog(value = "站点数据", desc = "同步数据", result = true)
//    @RequiresPermissions("sys:itemsData:add")
    @ResponseBody
    @RequestMapping("/syncData2")
    public String syncData2(HttpServletRequest request, @RequestParam(name = "platform", required = false)Integer platform
            , @RequestParam(name = "dateRange", required = false)String dateRange) {


//        System.out.println("syncData2接口调用了");

        syncGoogleItem();


        return "1";
    }


    /**
     * 动态列
     */
    @OperLog(value = "站点数据", desc = "动态列", result = true)
//    @RequiresPermissions("sys:itemsData:add")
    @ResponseBody
    @RequestMapping("/dynamicColumn")
    public JSONObject dynamicColumn(HttpServletRequest request, @RequestParam(name = "platform", required = false)Integer platform
            , @RequestParam(name = "dateRange", required = false)String dateRange) {

        User user = getLoginUser();
        Integer groupStatus = user.getGroupStatus();

        System.out.println("dynamicColumn start："+groupStatus);
//        subjectField = ["channelName", "goodsCost"];
//        subjectTitle = ["渠道", "商品成本"];

        JSONArray subjectTitleArr = new JSONArray();
        JSONArray subjectFieldArr = new JSONArray();


        String selectTypeStr = request.getParameter("selectType");
        Integer selectType =  StringUtils.isEmpty(selectTypeStr) ? 0 : Integer.valueOf(selectTypeStr);

        System.out.println("selectType："+selectType);
        System.out.println("groupStatus："+groupStatus);

        // selectType 0查询 1渠道分组
        // groupStatus 0不分组 1分组
        if(selectType == 1){
//            groupStatus = groupStatus == 0 ? 1 : 0;
            user.setGroupStatus(groupStatus == 0 ? 1 : 0);
        }

        System.out.println("groupStatus2："+user.getGroupStatus());


        if(user.getGroupStatus() == 1){
            subjectTitleArr.add("渠道");
            subjectFieldArr.add("channelName");
            System.out.println("dynamicColumn 分组");
        }else{
            System.out.println("dynamicColumn 不分组");
        }

        JSONObject dataObj = new JSONObject();
        dataObj.put("subjectTitle", subjectTitleArr);
        dataObj.put("subjectField", subjectFieldArr);

        System.out.println("dynamicColumn dataObj："+dataObj);
        return dataObj;
    }


    /**
     * 同步数据
     */
    @OperLog(value = "站点数据", desc = "同步数据", result = true)
    @RequiresPermissions("sys:itemsData:add")
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

            if(distanceOfTwoDate > 31){
                return JsonResult.error("日期范围不能大于31天");
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
