package com.egao.common.system.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.annotation.OperLog;
import com.egao.common.core.utils.AdEnum;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.web.BaseController;
import com.egao.common.core.web.JsonResult;
import com.egao.common.core.web.PageParam;
import com.egao.common.system.entity.Role;
import com.egao.common.system.entity.User;
import com.egao.common.system.mapper.AdMapper;
import com.egao.common.system.service.AdService;
import com.egao.common.system.service.ChannelService;
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
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Controller
@RequestMapping("/sys/channelData1016")
public class ChannelDataController1016 extends BaseController {

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
        String loginJobNumber = loginUser.getJobNumber();
        List<Role> rolesList = loginUser.getRoles();
        boolean isEmployee = true; // 是否员工
        for(Role  role : rolesList){
            if(role.getRoleId() < 3){
                isEmployee = false;
                break;
            }
        }



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
        map.put("jobNumber", isEmployee ? loginJobNumber : jobNumber);
        map.put("adAccount", adAccount);
        map.put("adChannel", adChannel);
        map.put("startTime", startTime);
        map.put("endTime", endTime);
        map.put("channelId", channelId);

        System.out.println("map:"+map);
//        List<Map<String, Object>> adList = adService.selectAdChannel(map);
        List<Map<String, Object>> adList = channelService.selectChannelData(map);
        System.out.println("+++++++++++adList："+JSONArray.toJSON(adList));

        int adCount = 0;
        Map<String, Object> totalRow = null;
        if(adList.size() > 0){
//            adCount = adService.selectAdChannelCount(map);
            adCount = channelService.selectChannelDataCount(map);

            map.put("page", 0);
            map.put("rows", 1000);
//            List<Map<String, Object>> adSumList = adService.selectAdChannel(map);
            List<Map<String, Object>> adSumList = channelService.selectChannelData(map);

            // 查询全部页汇总
//            totalRow = adService.selectAllSum(adSumList, map);


            List<Map<String, Object>> overallList = overallService.selectOverall(map);
            Float otherCostSum = 0f;
            if(overallList.size() > 0){

                Map<String, Object> overallMap = overallList.get(0);
                System.out.println("overallMap："+overallMap);
                BigDecimal adCostRatio = (BigDecimal)overallMap.get("adCostRatio");


                Float otherCost = 0f;
                Float gfCost = 0f;
                for(Map<String, Object> adMap : adList){
                    String sourceName = (String)adMap.get("sourceName");
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

                    if(StringUtils.isEmpty(sourceName)){
                        adMap.put("sourceName", "其他");

                    }

                }
                otherCostSum = otherCost - gfCost;
            }




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
        List<Map<String, Object>> lineList = adMapper.selectAdChannel(map);
        System.out.println("lineList2："+JSONArray.toJSON(lineList));
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
                        BigDecimal operateCostRatio = new BigDecimal(String.valueOf(lineMap.get("operateCostRatio")));
                        BigDecimal result = revenue.multiply(operateCostRatio); // 收入*运营成本占比

                        seriesList.add(result);
                        System.out.println("revenue："+revenue);
                        System.out.println("operateCostRatio："+operateCostRatio);
                        System.out.println("result："+result);
                    }

                }else if(keys.equals("profit")){ // 计算利润

                    BigDecimal revenue = new BigDecimal(String.valueOf(lineMap.get("revenue"))); // 收入
                    BigDecimal cost = new BigDecimal(String.valueOf(lineMap.get("cost"))); // 广告成本

                    BigDecimal logisticCost = new BigDecimal(String.valueOf(lineMap.get("logisticCost"))); // 物流成本
                    BigDecimal goodsCost = new BigDecimal(String.valueOf(lineMap.get("goodsCost"))); // 商品成本
                    BigDecimal operateCostRatio = new BigDecimal(String.valueOf(lineMap.get("operateCostRatio"))); // 运营成本占比
                    BigDecimal refund = new BigDecimal(String.valueOf(lineMap.get("refund"))); // 退款
                    BigDecimal toolCost = new BigDecimal(String.valueOf(lineMap.get("toolCost"))); // 工具成本
                    BigDecimal passCost = new BigDecimal(String.valueOf(lineMap.get("passCost"))); // 通道成本
                    // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
                    BigDecimal operateCost = revenue.multiply(operateCostRatio);
                    BigDecimal result = revenue.subtract(cost).subtract(logisticCost).subtract(goodsCost).subtract(refund).subtract(toolCost).subtract(passCost).subtract(operateCost);
                    seriesList.add(result);


                }else if(isKey(keys, lineMap)){
//                    System.out.println("--------------keys："+keys);
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

//            String source = (String)lineMap.get("source");
            String sourceName = (String)lineMap.get("source_name");
            xAxisData.add(StringUtils.isEmpty(sourceName) ? "其他" : sourceName);

        }

        lineMaps.put("legendData", legendData);
        lineMaps.put("seriesData", seriesData);
        lineMaps.put("xAxisData", xAxisData);


/*
        int adListSize = adList.size();
        for(int i = 0; i< adListSize; i++){

            Map<String, Object> adMaps = adList.get(i);

            System.out.println("adMaps："+adMaps);
            Integer parentId = (Integer)adMaps.get("parent_id");

            boolean exist = false;
            for(int j = 0; j < adList.size(); j++){
                Map<String, Object> adMap2 = adList.get(j);
                Integer id2 = (Integer)adMap2.get("id");
                if(id2.equals(parentId)){
                    System.out.println("存在存在......................终止");
                    exist = true;
                    break;
                }
            }
            if(!exist){
                final Map<String, Object> maps = channelService.selectChannelById(parentId);
                maps.put("source", maps.get("source_name"));
                adList.add(maps);
            }
        }
*/




//        adList.remove(adList.size()-1);

//        JsonResult data = JsonResult.ok(0, adCount,"成功").put("data", adList).put("lineData", lineMaps).put("totalRow", totalRow);
        JsonResult data = JsonResult.ok(0, adCount,"成功").put("data", adList).put("lineData", null).put("totalRow", totalRow);
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
