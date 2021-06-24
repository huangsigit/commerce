package com.egao.common.system.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.annotation.OperLog;
import com.egao.common.core.utils.AdEnum;
import com.egao.common.core.utils.ChannelEnum;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.utils.ListPageUtil;
import com.egao.common.core.web.BaseController;
import com.egao.common.core.web.JsonResult;
import com.egao.common.core.web.PageParam;
import com.egao.common.system.entity.Role;
import com.egao.common.system.entity.User;
import com.egao.common.system.mapper.AdMapper;
import com.egao.common.system.mapper.OverallMapper;
import com.egao.common.system.service.AdService;
import com.egao.common.system.service.ChannelService;
import com.egao.common.system.service.OverallService;
import org.apache.commons.lang3.StringUtils;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Controller
@RequestMapping("/sys/channelData")
public class ChannelDataController extends BaseController {

    @Autowired
    private AdService adService;

    @Autowired
    private AdMapper adMapper;

    @Autowired
    private OverallService overallService;

    @Autowired
    private ChannelService channelService;



    @RequiresPermissions("sys:channelData:view")
    @RequestMapping()
    public String view(Model model) {
        List<Map<String, Object>> channelList = channelService.selectChannelList(new HashMap());

        Map map = new HashMap();
        map.put("value", "0");
        map.put("name", "其它");
        channelList.add(map);

        model.addAttribute("channelList", JSON.toJSONString(channelList));

        return "system/channelData.html";
    }



    @OperLog(value = "渠道数据", desc = "分页查询")
    @RequiresPermissions("sys:channelData:list")
    @ResponseBody
    @RequestMapping("/page")
    public JsonResult channelData(HttpServletRequest request
            , @RequestParam(name = "page", required = false)Integer page, @RequestParam(name = "limit", required = false)Integer limit
            , @RequestParam(name = "itemsId", required = false)String itemsId, @RequestParam(name = "jobNumber", required = false)String jobNumber
            , @RequestParam(name = "adAccount", required = false)String adAccount, @RequestParam(name = "searchTime", required = false)String searchTime
            , @RequestParam(name = "adChannel", required = false)String adChannel, @RequestParam(name = "channelId", required = false)String channelId) {


        PageParam pageParam = new PageParam(request);
        pageParam.setDefaultOrder(new String[]{"id"}, null);

        System.out.println("分页查询渠道数据...");


        User loginUser = getLoginUser();
        Integer userId = loginUser.getUserId();
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

        searchTime = searchTime == null ? "" : searchTime;

        String startTime = StringUtils.substringBefore(searchTime, " - ");
        // 获取7天前日期
        startTime = StringUtils.isEmpty(startTime) ? DateUtil.timestampToTime(System.currentTimeMillis() - 86400000 * 7, "yyyy-MM-dd") : startTime;

        String endTime = StringUtils.substringAfter(searchTime, " - ");
        // 获取昨天日期
        endTime = StringUtils.isEmpty(endTime) ? DateUtil.timestampToTime(System.currentTimeMillis() - 86400000, "yyyy-MM-dd") : endTime;


        if(StringUtils.isNotEmpty(startTime) && StringUtils.isNotEmpty(endTime)){
            try {

                Date date = new Date(); //获取当前的系统时间。
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd") ; //使用了默认的格式创建了一个日期格式化对象。

                Date startDate = dateFormat.parse(startTime); //注意:指定的字符串格式必须要与SimpleDateFormat的模式要一致。
                System.out.println(startDate);
                Date endDate = dateFormat.parse(endTime); //注意:指定的字符串格式必须要与SimpleDateFormat的模式要一致。
                System.out.println(startDate.getTime());
                System.out.println(endDate.getTime());

                if(startDate.getTime() > endDate.getTime()){
                    return JsonResult.error("开始日期不能大于结束日期...");
                }

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }


        Map map = new HashMap();
//        map.put("page", 0);
//        map.put("rows", 10);
        map.put("page", (page-1)*limit);
        map.put("rows", limit);

        map.put("itemsId", itemsId);
//        map.put("jobNumber", jobNumber);
//        jobNumber = "无工号".equals(jobNumber) ? "000" : jobNumber;
        map.put("jobNumber", isEmployee ? loginJobNumber : jobNumber);
        map.put("adAccount", adAccount);
        map.put("adChannel", adChannel);
        map.put("startTime", startTime);
        map.put("endTime", endTime);
        map.put("channelId", channelId);
        map.put("userId", userId);

        System.out.println("map:"+map);
//        List<Map<String, Object>> adList = adService.selectAdChannel(map);
        List<Map<String, Object>> adList = channelService.selectChannelData(map);
        System.out.println("+++++++++++adList："+JSONArray.toJSON(adList));

        int adCount = 0;
        Map<String, Object> totalRow = null;
        if(adList.size() > 0){
//            adCount = adService.selectAdChannelCount(map);
//            adCount = channelService.selectChannelDataCount(map);

            map.put("page", 0);
            map.put("rows", 1000);
//            List<Map<String, Object>> adSumList = adService.selectAdChannel(map);
//            List<Map<String, Object>> channelList = channelService.selectChannelData(map);

            // 查询全部页汇总
//            totalRow = adService.selectAllSum(adSumList, map);
            totalRow = channelService.gatherChannelData(adList, map);

//            List<Map<String, Object>> overallList = overallService.selectOverall(map);
//            Float otherCostSum = 0f;
//            if(overallList.size() > 0){



                Float otherCost = 0f;
                Float gfCost = 0f;
                for(Map<String, Object> adMap : adList){
                    String channelName = (String)adMap.get("channelName");
                    String source = (String)adMap.get("source");
                    BigDecimal revenue = (BigDecimal)adMap.get("revenue");
                    BigDecimal cost = (BigDecimal)adMap.get("cost");


/*
                    if(source.contains("google") || source.contains("facebook")){
                        gfCost = gfCost + Float.valueOf(cost.toString());

                    }else{
                        otherCost = otherCost + Float.valueOf(revenue.toString()) * (Float.valueOf(adCostRatio.toString())/100);
                    }
*/

                    if(StringUtils.isEmpty(channelName)){
                        adMap.put("channelId", 0);
                        adMap.put("sourceName", "其他");

                    }

                }
//            }



/*
            Map otherMap = new HashMap();
            otherMap.put("source", "其他汇总");
            otherMap.put("revenue", "");
            otherMap.put("cost", otherCostSum);
            otherMap.put("logisticCost", "");
            otherMap.put("goodsCost", "");
            otherMap.put("operateCost", "");
            otherMap.put("refund", "");
            otherMap.put("toolCost", "");
            otherMap.put("passCost", "");
            otherMap.put("profit", "");


            Map<String, Object> testMap = adList.get(0);
            adList.add(otherMap);
*/

        }



        // 曲线图
        Map lineMaps = new HashMap();
//        List<Map<String, Object>> lineList = adService.selectAdSum(map);
//        List<Map<String, Object>> lineList = adMapper.selectAdChannel(map);
//        System.out.println("lineList2："+JSONArray.toJSON(lineList));
        List legendData = new ArrayList();
        List seriesData = new ArrayList();
        List xAxisData = new ArrayList();



        int adSize = ChannelEnum.values().length;
        System.out.println("ChannelEnum adSize："+adSize);
        for(int i = 1; i <= adSize; i++){
            String keys = ChannelEnum.getKeys(i);
            String values = ChannelEnum.getValues(i);

            System.out.println("ChannelEnum keys："+keys);
            System.out.println("ChannelEnum values："+values);


            legendData.add(values);


            List seriesList = new ArrayList();
//            for(Map<String, Object> lineMap : lineList){
            for(Map<String, Object> lineMap : adList){

                if(isKey(keys, lineMap)){ // 判断是否存在
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


        for(Map<String, Object> lineMap : adList){
            String channelName = (String)lineMap.get("channelName");
            xAxisData.add(StringUtils.isEmpty(channelName) ? "其他" : channelName);

        }


        lineMaps.put("legendData", legendData);
        lineMaps.put("seriesData", seriesData);
        lineMaps.put("xAxisData", xAxisData);

        List<Map<String, Object>> data1 = new ArrayList<>();
        if(adList.size() > 0){
            data1 = new ListPageUtil(adList, page, limit).getData();
            System.out.println("data data1："+JSONArray.toJSONString(data1));
        }


        JsonResult data = JsonResult.ok(0, adList.size(),"成功").put("data", data1).put("lineData", lineMaps).put("totalRow", totalRow);
//        JsonResult data = JsonResult.ok(0, 10,"成功").put("data", adList);
//        JsonResult data = JsonResult.ok(0, adList.size()).put("data", adList);

        System.out.println("channelData data:"+JSONObject.toJSON(data));
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

}
