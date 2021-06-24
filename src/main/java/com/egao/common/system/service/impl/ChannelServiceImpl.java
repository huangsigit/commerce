package com.egao.common.system.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.AdConstant;
import com.egao.common.core.UploadConstant;
import com.egao.common.core.utils.AnalyticsUtil;
import com.egao.common.core.utils.CostUtil;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.utils.HttpUtil;
import com.egao.common.system.mapper.AdMapper;
import com.egao.common.system.mapper.ChannelMapper;
import com.egao.common.system.mapper.OverallMapper;
import com.egao.common.system.service.CertificateService;
import com.egao.common.system.service.ChannelCostService;
import com.egao.common.system.service.ChannelService;
import com.google.api.services.analytics.model.AccountSummary;
import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.annotations.Param;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * 渠道表
 * </p>
 *
 * @author hs
 * @since 2020-10-10
 */
@Service
public class ChannelServiceImpl implements ChannelService {

    private Logger logger = LoggerFactory.getLogger("ChannelServiceImpl");

    @Autowired
    public ChannelMapper channelMapper;

    @Autowired
    public AdMapper adMapper;

    @Autowired
    private ChannelCostService channelCostService;

    @Autowired
    public OverallMapper overallMapper;


    public List<Map<String, Object>> selectChannel(Map map){
        List<Map<String, Object>> channelList = channelMapper.selectChannel(map);
        return channelList;
    }

    public int selectChannelCount(Map map){
        int channelCount = channelMapper.selectChannelCount(map);
        return channelCount;
    }

    public List<Map<String, Object>> selectChannelList(Map map){
        List<Map<String, Object>> channelList = channelMapper.selectChannelList(map);
        return channelList;
    }


    public List<Map<String, Object>> selectCanBindChannel(Map map){
        List<Map<String, Object>> channelList = channelMapper.selectCanBindChannel(map);
        return channelList;
    }

    public Map<String, Object> selectChannelById(Integer id){
        Map<String, Object> channelMap = channelMapper.selectChannelById(id);
        return channelMap;
    }

    public List<Map<String, Object>> selectChannelByParentId(Integer parentId){
        List<Map<String, Object>> channelMap = channelMapper.selectChannelByParentId(parentId);
        return channelMap;
    }


    @Override
    public int insertChannel(Map map){
        int count = channelMapper.insertChannel(map);
        Integer id = (Integer)map.get("id");
        return id;
    }

    @Override
    public void updateChannel(Map map){
        channelMapper.updateChannel(map);
    }

    @Override
    public void deleteById(Integer id){
        channelMapper.deleteById(id);
    }

    @Override
    public void deleteByParentId(Integer parentId){

        channelMapper.deleteByParentId(parentId);

    }

    public List<Map<String, Object>> selectChannelCost(Map map){
        List<Map<String, Object>> costList = channelMapper.selectChannelCost(map);
        return costList;
    }

    public List<Map<String, Object>> selectChannelRevenue(Map map){
        List<Map<String, Object>> revenueList = channelMapper.selectChannelRevenue(map);
        return revenueList;
    }

    public int selectChannelDataCount(Map map){
        int adCount = channelMapper.selectChannelDataCount(map);
        return adCount;
    }

    public Map<String, Object> gatherChannelData(List<Map<String, Object>> adList, Map map){
        System.out.println("gatherChannelData 开始汇总...");

        map.put("page", 0);
        map.put("rows", 1000);


        BigDecimal revenue = new BigDecimal(0);
        BigDecimal roas = new BigDecimal(0);
        BigDecimal cost = new BigDecimal(0);
        BigDecimal costProportion = new BigDecimal(0);
        BigDecimal profit = new BigDecimal(0);



//        List<Map<String, Object>> adList = selectAd(map);
        System.out.println("///////////////////////adList："+adList);
        for(Map<String, Object> adMap : adList){
//            System.out.println("********adMap："+JSONObject.toJSON(adMap));
            revenue = revenue.add((BigDecimal)adMap.get("revenue"));
//            roas = roas.add((BigDecimal)adMap.get("roas"));
            cost = cost.add((BigDecimal)adMap.get("cost"));
//            costProportion = costProportion.add((BigDecimal)adMap.get("costProportion"));

            profit = profit.add((BigDecimal)adMap.get("profit"));
            BigDecimal profitRate = new BigDecimal(0);

            BigDecimal change = new BigDecimal(100);
            BigDecimal init = new BigDecimal(0.00);

            // ROAS
            boolean zero = cost.compareTo(BigDecimal.ZERO)==0;
            boolean isRevenue = revenue.compareTo(BigDecimal.ZERO)==0;
//            System.out.println("zero:"+zero);

            roas = new BigDecimal(0.00);
            roas.setScale(2, BigDecimal.ROUND_HALF_UP);
            if(zero){
//                adMap.put("roas", roas);
            }else{
                roas = revenue.divide(cost, 2, RoundingMode.HALF_UP);
            }


            map.put("revenue", "$" + revenue + " (" + roas +  ")");
            map.put("roas", roas);
            costProportion = isRevenue? change : (cost.divide(revenue, 4, RoundingMode.HALF_UP)).multiply(change).setScale(2, RoundingMode.HALF_UP);



            map.put("cost", "$" + roundHalfUp(cost) + " (" + costProportion + "%" +  ")");
            map.put("costProportion", costProportion);


            profitRate = isRevenue? init : profit.divide(revenue, 4, BigDecimal.ROUND_HALF_UP);
            profitRate = profitRate.multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP);
            map.put("profit", "$" + roundHalfUp(profit) + " (" + profitRate + "%" +  ")");
            map.put("profitRate", profitRate);

        }

        return map;
    }


    public BigDecimal roundHalfUp(BigDecimal value){
        return value.setScale(2, BigDecimal.ROUND_HALF_UP);
    }

/*

    public List<Map<String, Object>> selectChannelData(Map map){


        String startTime = (String)map.get("startTime");
        String endTime = (String)map.get("endTime");

        System.out.println("--->selectItemData start："+map);
        System.out.println("startTime："+startTime);
        System.out.println("endTime："+endTime);
        long startTimeLong = DateUtil.parseDate(startTime, "yyyy-MM-dd").getTime();
        long endTimeLong = DateUtil.parseDate(endTime, "yyyy-MM-dd").getTime();
        String startMonth = DateUtil.timestampToTime(DateUtil.parseDate(startTime, "yyyy-MM-dd").getTime(), "yyyy-MM");
        String endMonth = DateUtil.timestampToTime(DateUtil.parseDate(endTime, "yyyy-MM-dd").getTime(), "yyyy-MM");




        boolean monthIfEquals = DateUtil.monthIfEquals(startTime, endTime); // 比如两个日期月份是否相等
        System.out.println("monthIfEquels：" + monthIfEquals);
//        List<Map<String, Object>> itemList = adMapper.selectItemData(map);
//        List<Map<String, Object>> channelList = adMapper.selectChannelData(map);
        List<Map<String, Object>> channelList = channelMapper.selectChannelData(map);


        System.out.println("//channelList："+channelList);

        Map map2 = new HashMap();
        map2.put("startTime", startMonth);
        map2.put("endTime", endMonth);
        map2.put("page", 0);
        map2.put("rows", 1000);
//        List<Map<String, Object>> costList = costService.selectCost(map2);
//        List<Map<String, Object>> costList = channelCostService.selectChannelCost(map2);
        List<Map<String, Object>> costList = channelCostService.selectChannelAdCost(map);

        System.out.println("+++selectChannelData costList："+JSONArray.toJSONString(costList));

        List<Map<String, Object>> tempList = new ArrayList<>();

//        List<Map<String, Object>> overallList = overallMapper.selectOverall(map2);


        String aMonthFirstDayTime = DateUtil.getAMonthFirstDay(startTimeLong); // 获取一个月第一天
        String aMonthLastDayTime = DateUtil.getAMonthLastDay(endTimeLong); // 获取一个月最后一天
        Map map3 = new HashMap();
        map3.put("itemsId", map.get("itemsId"));
        map3.put("startTime", aMonthFirstDayTime);
        map3.put("endTime", aMonthLastDayTime);
        map3.put("jobNumer", map.get("jobNumer"));
        map3.put("adAccount", map.get("adAccount"));
        map3.put("channelId", map.get("channelId"));

        System.out.println("map388："+map3);
        List<Map<String, Object>> channelRevenueList = channelMapper.selectChannelRevenue(map3);

        CostUtil.censusChannelCost(tempList, channelList, costList, channelRevenueList, startTime, endTime);
        return tempList;
    }
*/


    public List<Map<String, Object>> selectChannelData(Map map){

        String startTime = (String)map.get("startTime");
        String endTime = (String)map.get("endTime");

        System.out.println("--->selectItemData start："+map);
        System.out.println("startTime："+startTime);
        System.out.println("endTime："+endTime);


        List<Map<String, Object>> channelList = channelMapper.selectChannelData(map);
        System.out.println("channelList："+JSONArray.toJSONString(channelList));


        List<Map<String, Object>> tempList = new ArrayList<>();


        for(Map<String, Object> channalMap : channelList){
            Integer channelId = (Integer)channalMap.getOrDefault("channelId", 0);
            String channelName = (String)channalMap.getOrDefault("channelName", "其它");
            BigDecimal revenue = (BigDecimal)channalMap.get("revenue");
            BigDecimal cost = (BigDecimal)channalMap.get("cost");
            BigDecimal channelCost = (BigDecimal)channalMap.get("channelCost");
//            BigDecimal channelCost = new BigDecimal(channelCostDouble);

            System.out.println("BigDecimal.ZERO.equals(channelCost)0:"+channelCost);
            System.out.println("BigDecimal.ZERO.equals(channelCost):"+BigDecimal.ZERO.equals(channelCost));
            System.out.println("222:"+channelCost == null);
            System.out.println("333:"+String.valueOf(channelCost).equals("null"));
            System.out.println("444："+ StringUtils.isEmpty(String.valueOf(channelCost)));

            channelCost = String.valueOf(channelCost).equals("null") ? new BigDecimal(0.00) : channelCost;

            channalMap.put("channelId", channelId);
            channalMap.put("channelName", channelName);

            System.out.println("111channalMap："+channalMap);

            BigDecimal change = new BigDecimal(100);

            cost = cost.add(channelCost);


            // ROAS
            boolean costZero = cost.compareTo(BigDecimal.ZERO)==0;
            channalMap.put("roas", costZero ? cost : revenue.divide(cost,2, RoundingMode.HALF_UP));

            // 广告成本占比
            boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;
            channalMap.put("costProportion", revenueZero ? revenueZero : cost.divide(revenue,6, RoundingMode.HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));


            channalMap.put("cost", cost.setScale(2, BigDecimal.ROUND_HALF_UP));

            // 利润
            // 收入-广告成本
            BigDecimal profit = revenue.subtract(cost);

            // 利润
            BigDecimal profitRou = roundHalfUp(profit.setScale(2, BigDecimal.ROUND_HALF_UP));
            channalMap.put("profit", profitRou);

            // 利润率
            // 利润/收入
            BigDecimal profitRate = new BigDecimal(0.00);
            // 判断被除数是否为零
            profitRate = revenueZero ? profitRate : profitRou.divide(revenue, 6, BigDecimal.ROUND_HALF_UP);

            BigDecimal profitRateRou = profitRate.multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP);

            channalMap.put("profitRate", profitRateRou);

            channalMap.put("revenue", revenue.setScale(2, BigDecimal.ROUND_HALF_UP));

        }

        return channelList;
    }


}
