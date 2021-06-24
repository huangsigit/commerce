package com.egao.common.system.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.UploadConstant;
import com.egao.common.core.utils.AnalyticsUtil;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.utils.HttpUtil;
import com.egao.common.system.mapper.AdMapper;
import com.egao.common.system.mapper.OverallMapper;
import com.egao.common.system.service.AdService;
import com.egao.common.system.service.CertificateService;
import com.egao.common.system.service.ChannelCostService;
import com.egao.common.system.service.CostService;
import com.google.api.services.analytics.model.AccountSummary;
import com.google.api.services.analytics.model.ProfileSummary;
import com.google.api.services.analytics.model.WebPropertySummary;
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
import java.util.*;

/**
 * <p>
 * 菜单表 服务实现类
 * </p>
 *
 * @author hs
 * @since 2020-10-10
 */
@Service
public class AdServiceImpl implements AdService {

    private Logger logger = LoggerFactory.getLogger("AdServiceImpl");

    @Autowired
    public AdMapper adMapper;

    @Autowired
    public OverallMapper overallMapper;

    @Autowired
    public CertificateService certificateService;

/*
    @Autowired
    private CostService costService;

    @Autowired
    private ChannelCostService channelCostService;
*/


    public static String GRAPH_URL = "https://graph.facebook.com/v7.0/";

    public static String ACCESS_TOKEN = "EAAH92JtasVMBAJ2iHbMXEdLwzMZAH2PidkMGwvQbhFZCZAAcPmUHOxfwaPfNg4M3vXCBonOVZAHLIrj7gdZCJqT9pQs8CAMGrBp7ECuNKOdFIO5txnP3UylNAI959oXBqp1hZAJloEBqSvVdt3hVhXYDu7WGdoZCgZCqrqX0PVE5LKKdGtlzQMxZBmrY8YWjQARUZD";

    public static String BUSINESS_ID = "144436283227029";





    public List<Map<String, Object>> selectAd(Map map){


        List<Map<String, Object>> adList = adMapper.selectAd(map);
        System.out.println("selectAd adList："+ JSONArray.toJSONString(adList));
        Map oMap = new HashMap();
        oMap.put("page", 0);
        oMap.put("rows", 10);
        List<Map<String, Object>> overallList = overallMapper.selectOverall(oMap);
        System.out.println("overallList："+overallList);

        BigDecimal change = new BigDecimal(100);
        for(Map<String, Object> adMap : adList){
            BigDecimal revenue = (BigDecimal)adMap.get("revenue");
            BigDecimal cost = (BigDecimal)adMap.get("cost");


            // ROAS
            boolean zero = cost.compareTo(BigDecimal.ZERO)==0;
            BigDecimal roas = new BigDecimal(0.00);
            roas.setScale(2, BigDecimal.ROUND_HALF_UP);
            if(zero){
                adMap.put("roas", roas);
            }else{
                adMap.put("roas", revenue.divide(cost,2, RoundingMode.HALF_UP));
                roas = revenue.divide(cost, 2, RoundingMode.HALF_UP);
            }

            // 广告成本占比
            boolean zero2 = revenue.compareTo(BigDecimal.ZERO)==0;
            BigDecimal costProportion = new BigDecimal(0.00);
            costProportion.setScale(4, BigDecimal.ROUND_HALF_UP);
//            System.out.println("zero:"+zero);
            if(zero2){
                adMap.put("costProportion", costProportion);
//                costProportion = "0.00;
            }else{
                adMap.put("costProportion", (cost.divide(revenue,4, RoundingMode.HALF_UP)).multiply(change));
                costProportion = cost.divide(revenue,2, RoundingMode.HALF_UP);
            }


//            adMap.put("revenue", "$" + revenue + " (" + roas + "%" +  ")");
            adMap.put("revenue", revenue);
//            adMap.put("cost", "$" + cost + " (" + costProportion + "%" +  ")");
            adMap.put("cost",  cost);

            System.out.println("++++++++++++++adMap:"+ JSONObject.toJSON(adMap));
//            BigDecimal operateCost = new BigDecimal(operateCostStr);// 运营成本

            BigDecimal operateCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("operateCostRatio", 0)));// 运营成本占比
//            BigDecimal logisticCost = new BigDecimal(String.valueOf(adMap.getOrDefault("logisticCost", 0))); // 物流成本
            BigDecimal logisticCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("logisticCostRatio", 0))); // 物流成本占比
//            BigDecimal goodsCost = new BigDecimal(String.valueOf(adMap.getOrDefault("goodsCost", 0))); // 商品成本
            BigDecimal goodsCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("goodsCostRatio", 0))); // 商品成本占比
//            BigDecimal refund = new BigDecimal(String.valueOf(adMap.getOrDefault("refund", 0))); // 退款
            BigDecimal refundRate = new BigDecimal(String.valueOf(adMap.getOrDefault("refundRate",0))); // 退款率
//            BigDecimal toolCost = new BigDecimal(String.valueOf(adMap.getOrDefault("toolCost", 0))); // 工具成本
            BigDecimal toolCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("toolCostRatio", 0))); // 工具成本占比
//            BigDecimal passCost = new BigDecimal(String.valueOf(adMap.getOrDefault("passCost", 0))); // 通道成本
            BigDecimal passCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("passCostRatio", 0))); // 通道成本占比

//            BigDecimal operateCost = revenue.multiply(operateCostRatio).setScale(2, BigDecimal.ROUND_HALF_UP);


/*
            // 取全局
            boolean logisticCostRatioBoo = logisticCostRatio.compareTo(BigDecimal.ZERO)==0;
            boolean goodsCostRatioBoo = goodsCostRatio.compareTo(BigDecimal.ZERO)==0;
            boolean refundRateBoo = refundRate.compareTo(BigDecimal.ZERO)==0;
            boolean operateCostRatioBoo = operateCostRatio.compareTo(BigDecimal.ZERO)==0;
            boolean toolCostRatioBoo = toolCostRatio.compareTo(BigDecimal.ZERO)==0;
            boolean passCostRatioBoo = passCostRatio.compareTo(BigDecimal.ZERO)==0;

            if(logisticCostRatioBoo && goodsCostRatioBoo && refundRateBoo
                    && operateCostRatioBoo && toolCostRatioBoo && passCostRatioBoo){
                System.out.println("都是0.00-----------");

//                Map<String, Object> overallMap = overallList.get(0);
                Map<String, Object> overallMap = adMap;

                System.out.println("if overallMap："+overallMap);

                logisticCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("logisticCostRatioAll", 0)));// 物流成本占比
                goodsCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("goodsCostRatioAll", 0))); // 商品成本占比
                refundRate = new BigDecimal(String.valueOf(overallMap.getOrDefault("refundRateAll", 0))); // 退款率
                toolCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("toolCostRatioAll", 0))); // 工具成本占比
                passCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("passCostRatioAll", 0))); // 通道成本占比
                operateCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("operateCostRatioAll", 0))); // 运营成本占比

                logisticCost = new BigDecimal(String.valueOf(overallMap.getOrDefault("logisticCostAll", 0)));// 物流成本占比
                goodsCost = new BigDecimal(String.valueOf(overallMap.getOrDefault("goodsCostAll", 0))); // 商品成本占比
                refund = new BigDecimal(String.valueOf(overallMap.getOrDefault("refundAll", 0))); // 退款率
                toolCost = new BigDecimal(String.valueOf(overallMap.getOrDefault("toolCostAll", 0))); // 工具成本占比
                passCost = new BigDecimal(String.valueOf(overallMap.getOrDefault("passCostAll", 0))); // 通道成本占比
                operateCost = new BigDecimal(String.valueOf(overallMap.getOrDefault("operateCostAll", 0))); // 运营成本占比


                adMap.put("logisticCostRatio", roundHalfUp(logisticCostRatio));
                adMap.put("goodsCostRatio", roundHalfUp(goodsCostRatio));
                adMap.put("refundRate", roundHalfUp(refundRate));
                adMap.put("operateCostRatio", roundHalfUp(operateCostRatio));
                adMap.put("toolCostRatio", roundHalfUp(toolCostRatio));
                adMap.put("passCostRatio", roundHalfUp(passCostRatio));

                adMap.put("logisticCost", roundHalfUp(logisticCost));
                adMap.put("goodsCost", roundHalfUp(goodsCost));
                adMap.put("refund", roundHalfUp(refund));
                adMap.put("operateCost", roundHalfUp(operateCost));
                adMap.put("toolCost", roundHalfUp(toolCost));
                adMap.put("passCost", roundHalfUp(passCost));

                System.out.println("adMap："+adMap);

                System.out.println("logisticCostRatio："+logisticCostRatio);
                System.out.println("revenue："+revenue);
                System.out.println("logisticCost："+logisticCost);
                System.out.println("-------------------------");


//                logisticCostRatio = logisticCostRatio.divide(100,2, RoundingMode.HALF_UP));
                BigDecimal convert = new BigDecimal(100);
                BigDecimal logisticCostRatioConvert = logisticCostRatio.divide(convert,2, RoundingMode.HALF_UP);
                BigDecimal goodsCostRatioConvert = goodsCostRatio.divide(convert,2, RoundingMode.HALF_UP);
                BigDecimal refundRateConvert = refundRate.divide(convert,2, RoundingMode.HALF_UP);
                BigDecimal toolCostRatioConvert = toolCostRatio.divide(convert,2, RoundingMode.HALF_UP);
                BigDecimal passCostRatioConvert = passCostRatio.divide(convert,2, RoundingMode.HALF_UP);
                BigDecimal operateCostRatioConvert = operateCostRatio.divide(convert,2, RoundingMode.HALF_UP);

//                BigDecimal bd = new BigDecimal(1000);
                BigDecimal bd = revenue;
                logisticCost = logisticCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
                goodsCost = goodsCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
                refund = refundRateConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
                toolCost = toolCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
                passCost = passCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
                operateCost = operateCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);


                System.out.println("logisticCost2："+logisticCost);
                System.out.println("-------------------------");

            }
*/




            // 物流成本
//            adMap.put("logisticCost", "$" + logisticCost + " (" + logisticCostRatio + "%" +  ")");
//            adMap.put("logisticCost", roundHalfUp(logisticCost));


            BigDecimal logisticCost = roundHalfUp(revenue.multiply(logisticCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP)));

            System.out.println("---》revenue："+revenue);
            System.out.println("logisticCostRatio："+logisticCostRatio);
            System.out.println("logisticCostRatio.divide(change, 2, BigDecimal.ROUND_HALF_UP)："+logisticCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP));
            System.out.println("《---logisticCost："+logisticCost);

            adMap.put("logisticCost", logisticCost);
            adMap.put("logisticCostRatio", roundHalfUp(logisticCostRatio));
            // 商品成本
//            adMap.put("goodsCost", "$" + goodsCost + " (" + goodsCostRatio + "%" +  ")");
            BigDecimal goodsCost = roundHalfUp(revenue.multiply(goodsCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP)));
            adMap.put("goodsCost", goodsCost);
            adMap.put("goodsCostRatio", roundHalfUp(goodsCostRatio));
            // 运营成本
//            adMap.put("operateCost", "$" + operateCost + " (" + operateCostRatio + "%" +  ")");
//            System.out.println("---------operateCostDouble1："+operateCost);
//            double operateCostDouble = operateCost.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
//            System.out.println("operateCostDouble2："+operateCostDouble);
            BigDecimal operateCost = roundHalfUp(revenue.multiply(operateCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP)));
            adMap.put("operateCost", operateCost);
            adMap.put("operateCostRatio", roundHalfUp(operateCostRatio));
            // 退款
//            adMap.put("refund", "$" + refund + " (" + refundRate + "%" +  ")");
            BigDecimal refund = roundHalfUp(revenue.multiply(refundRate.divide(change, 4, BigDecimal.ROUND_HALF_UP)));
            adMap.put("refund", refund);
            adMap.put("refundRate", roundHalfUp(refundRate));
            // 工具成本
//            adMap.put("toolCost", "$" + toolCost + " (" + toolCostRatio + "%" +  ")");
            BigDecimal toolCost = roundHalfUp(revenue.multiply(toolCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP)));
            adMap.put("toolCost", toolCost);
            adMap.put("toolCostRatio", roundHalfUp(toolCostRatio));
            // 通道成本
//            adMap.put("passCost", "$" + passCost + " (" + passCostRatio + "%" +  ")");
            BigDecimal passCost = roundHalfUp(revenue.multiply(passCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP)));
            adMap.put("passCost", passCost);
            adMap.put("passCostRatio", roundHalfUp(passCostRatio));

            // 运营成本

//            adMap.put("operateCost", roundHalfUp(operateCost)); // 收入*运营成本占比


            // 利润
            // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
            BigDecimal profit = revenue.subtract(cost).subtract(logisticCost).subtract(goodsCost).subtract(operateCost).subtract(refund).subtract(toolCost).subtract(passCost);

            // 利润率
            // 利润/收入
            boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;
//            System.out.println("zero:"+zero);
            BigDecimal profitRate = new BigDecimal(0.00);

            if(revenueZero){
//                adMap.put("profitRate", "$" + "0");
            }else{
                profitRate = profit.divide(revenue, 4, BigDecimal.ROUND_HALF_UP);
//                adMap.put("profitRate", "$" + profitRate);
            }

            adMap.put("profitRate", roundHalfUp(profitRate.multiply(change)));

//            double profitValue = profit.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            // 利润
//            adMap.put("profit", "$" + profit + " (" + profitRate + "%" +  ")");
            adMap.put("profit", roundHalfUp(profit));

            System.out.println("---------------adMap："+adMap);

        }

        System.out.println("adServuceImpl adList："+JSONArray.toJSON(adList));

        return adList;
    }



/*
    public List<Map<String, Object>> selectAd(Map map){

        List<Map<String, Object>> adList = adMapper.selectAd(map);
        Map oMap = new HashMap();
        oMap.put("page", 0);
        oMap.put("rows", 10);
        List<Map<String, Object>> overallList = overallMapper.selectOverall(oMap);
        System.out.println("overallList："+overallList);

        for(Map<String, Object> adMap : adList){
            BigDecimal revenue = (BigDecimal)adMap.get("revenue");
            BigDecimal cost = (BigDecimal)adMap.get("cost");


            // ROAS
            boolean zero = cost.compareTo(BigDecimal.ZERO)==0;
//            System.out.println("zero:"+zero);

            BigDecimal roas = new BigDecimal(0.00);
            roas.setScale(2, BigDecimal.ROUND_HALF_UP);
            if(zero){
                adMap.put("roas", roas);
            }else{
                adMap.put("roas", revenue.divide(cost,2, RoundingMode.HALF_UP));
                roas = revenue.divide(cost, 2, RoundingMode.HALF_UP);
            }

            // 广告成本占比
            boolean zero2 = revenue.compareTo(BigDecimal.ZERO)==0;
            BigDecimal costProportion = new BigDecimal(0.00);
            costProportion.setScale(2, BigDecimal.ROUND_HALF_UP);
//            System.out.println("zero:"+zero);
            if(zero2){
                adMap.put("costProportion", costProportion);
//                costProportion = "0.00;
            }else{
                adMap.put("costProportion", cost.divide(revenue,2, RoundingMode.HALF_UP));
                costProportion = cost.divide(revenue,2, RoundingMode.HALF_UP);
            }


//            adMap.put("revenue", "$" + revenue + " (" + roas + "%" +  ")");
            adMap.put("revenue", revenue);
//            adMap.put("cost", "$" + cost + " (" + costProportion + "%" +  ")");
            adMap.put("cost",  cost);

            System.out.println("++++++++++++++adMap:"+ JSONObject.toJSON(adMap));
//            BigDecimal operateCost = new BigDecimal(operateCostStr);// 运营成本

            BigDecimal operateCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("operateCostRatio", 0)));// 运营成本占比
            BigDecimal logisticCost = new BigDecimal(String.valueOf(adMap.getOrDefault("logisticCost", 0))); // 物流成本
            BigDecimal logisticCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("logisticCostRatio", 0))); // 物流成本占比
            BigDecimal goodsCost = new BigDecimal(String.valueOf(adMap.getOrDefault("goodsCost", 0))); // 商品成本
            BigDecimal goodsCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("goodsCostRatio", 0))); // 商品成本占比
            BigDecimal refund = new BigDecimal(String.valueOf(adMap.getOrDefault("refund", 0))); // 退款
            BigDecimal refundRate = new BigDecimal(String.valueOf(adMap.getOrDefault("refundRate",0))); // 退款率
            BigDecimal toolCost = new BigDecimal(String.valueOf(adMap.getOrDefault("toolCost", 0))); // 工具成本
            BigDecimal toolCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("toolCostRatio", 0))); // 工具成本占比
            BigDecimal passCost = new BigDecimal(String.valueOf(adMap.getOrDefault("passCost", 0))); // 通道成本
            BigDecimal passCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("passCostRatio", 0))); // 通道成本占比

            BigDecimal operateCost = revenue.multiply(operateCostRatio).setScale(2, BigDecimal.ROUND_HALF_UP);

            // 取全局
            boolean logisticCostRatioBoo = logisticCostRatio.compareTo(BigDecimal.ZERO)==0;
            boolean goodsCostRatioBoo = goodsCostRatio.compareTo(BigDecimal.ZERO)==0;
            boolean refundRateBoo = refundRate.compareTo(BigDecimal.ZERO)==0;
            boolean operateCostRatioBoo = operateCostRatio.compareTo(BigDecimal.ZERO)==0;
            boolean toolCostRatioBoo = toolCostRatio.compareTo(BigDecimal.ZERO)==0;
            boolean passCostRatioBoo = passCostRatio.compareTo(BigDecimal.ZERO)==0;

            if(logisticCostRatioBoo && goodsCostRatioBoo && refundRateBoo
                    && operateCostRatioBoo && toolCostRatioBoo && passCostRatioBoo){
                System.out.println("都是0.00-----------");

//                Map<String, Object> overallMap = overallList.get(0);
                Map<String, Object> overallMap = adMap;

                System.out.println("if overallMap："+overallMap);

                logisticCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("logisticCostRatioAll", 0)));// 物流成本占比
                goodsCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("goodsCostRatioAll", 0))); // 商品成本占比
                refundRate = new BigDecimal(String.valueOf(overallMap.getOrDefault("refundRateAll", 0))); // 退款率
                toolCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("toolCostRatioAll", 0))); // 工具成本占比
                passCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("passCostRatioAll", 0))); // 通道成本占比
                operateCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("operateCostRatioAll", 0))); // 运营成本占比

                logisticCost = new BigDecimal(String.valueOf(overallMap.getOrDefault("logisticCostAll", 0)));// 物流成本占比
                goodsCost = new BigDecimal(String.valueOf(overallMap.getOrDefault("goodsCostAll", 0))); // 商品成本占比
                refund = new BigDecimal(String.valueOf(overallMap.getOrDefault("refundAll", 0))); // 退款率
                toolCost = new BigDecimal(String.valueOf(overallMap.getOrDefault("toolCostAll", 0))); // 工具成本占比
                passCost = new BigDecimal(String.valueOf(overallMap.getOrDefault("passCostAll", 0))); // 通道成本占比
                operateCost = new BigDecimal(String.valueOf(overallMap.getOrDefault("operateCostAll", 0))); // 运营成本占比


                adMap.put("logisticCostRatio", roundHalfUp(logisticCostRatio));
                adMap.put("goodsCostRatio", roundHalfUp(goodsCostRatio));
                adMap.put("refundRate", roundHalfUp(refundRate));
                adMap.put("operateCostRatio", roundHalfUp(operateCostRatio));
                adMap.put("toolCostRatio", roundHalfUp(toolCostRatio));
                adMap.put("passCostRatio", roundHalfUp(passCostRatio));

                adMap.put("logisticCost", roundHalfUp(logisticCost));
                adMap.put("goodsCost", roundHalfUp(goodsCost));
                adMap.put("refund", roundHalfUp(refund));
                adMap.put("operateCost", roundHalfUp(operateCost));
                adMap.put("toolCost", roundHalfUp(toolCost));
                adMap.put("passCost", roundHalfUp(passCost));

                System.out.println("adMap："+adMap);

                System.out.println("logisticCostRatio："+logisticCostRatio);
                System.out.println("revenue："+revenue);
                System.out.println("logisticCost："+logisticCost);
                System.out.println("-------------------------");


//                logisticCostRatio = logisticCostRatio.divide(100,2, RoundingMode.HALF_UP));
                BigDecimal convert = new BigDecimal(100);
                BigDecimal logisticCostRatioConvert = logisticCostRatio.divide(convert,2, RoundingMode.HALF_UP);
                BigDecimal goodsCostRatioConvert = goodsCostRatio.divide(convert,2, RoundingMode.HALF_UP);
                BigDecimal refundRateConvert = refundRate.divide(convert,2, RoundingMode.HALF_UP);
                BigDecimal toolCostRatioConvert = toolCostRatio.divide(convert,2, RoundingMode.HALF_UP);
                BigDecimal passCostRatioConvert = passCostRatio.divide(convert,2, RoundingMode.HALF_UP);
                BigDecimal operateCostRatioConvert = operateCostRatio.divide(convert,2, RoundingMode.HALF_UP);

//                BigDecimal bd = new BigDecimal(1000);
                BigDecimal bd = revenue;
                logisticCost = logisticCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
                goodsCost = goodsCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
                refund = refundRateConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
                toolCost = toolCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
                passCost = passCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
                operateCost = operateCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);


                System.out.println("logisticCost2："+logisticCost);
                System.out.println("-------------------------");

            }


            // 运营成本

            adMap.put("operateCost", roundHalfUp(operateCost)); // 收入*运营成本占比

            // 利润
            // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
            BigDecimal profit = revenue.subtract(cost).subtract(logisticCost).subtract(goodsCost).subtract(operateCost).subtract(refund).subtract(toolCost).subtract(passCost);

            // 利润率
            // 利润/收入
            boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;
//            System.out.println("zero:"+zero);
            BigDecimal profitRate = new BigDecimal(0.00);
            if(revenueZero){
//                adMap.put("profitRate", "$" + "0");
            }else{
                profitRate = profit.divide(revenue, 2, BigDecimal.ROUND_HALF_UP);
//                adMap.put("profitRate", "$" + profitRate);
            }

            adMap.put("profitRate", roundHalfUp(profitRate));

            // 物流成本
//            adMap.put("logisticCost", "$" + logisticCost + " (" + logisticCostRatio + "%" +  ")");
            adMap.put("logisticCost", roundHalfUp(logisticCost));
            adMap.put("logisticCostRatio", roundHalfUp(logisticCostRatio));
            // 商品成本
//            adMap.put("goodsCost", "$" + goodsCost + " (" + goodsCostRatio + "%" +  ")");
            adMap.put("goodsCost", roundHalfUp(goodsCost));
            adMap.put("goodsCostRatio", roundHalfUp(goodsCostRatio));
            // 运营成本
//            adMap.put("operateCost", "$" + operateCost + " (" + operateCostRatio + "%" +  ")");
//            System.out.println("---------operateCostDouble1："+operateCost);
//            double operateCostDouble = operateCost.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
//            System.out.println("operateCostDouble2："+operateCostDouble);
            adMap.put("operateCost", roundHalfUp(operateCost));
            adMap.put("operateCostRatio", roundHalfUp(operateCostRatio));
            // 退款
//            adMap.put("refund", "$" + refund + " (" + refundRate + "%" +  ")");
            adMap.put("refund", roundHalfUp(refund));
            adMap.put("refundRate", roundHalfUp(refundRate));
            // 工具成本
//            adMap.put("toolCost", "$" + toolCost + " (" + toolCostRatio + "%" +  ")");
            adMap.put("toolCost", roundHalfUp(toolCost));
            adMap.put("toolCostRatio", roundHalfUp(toolCostRatio));
            // 通道成本
//            adMap.put("passCost", "$" + passCost + " (" + passCostRatio + "%" +  ")");
            adMap.put("passCost", roundHalfUp(passCost));
            adMap.put("passCostRatio", roundHalfUp(passCostRatio));

//            double profitValue = profit.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            // 利润
//            adMap.put("profit", "$" + profit + " (" + profitRate + "%" +  ")");
            adMap.put("profit", roundHalfUp(profit));

            System.out.println("---------------adMap："+adMap);

        }

        System.out.println("adServuceImpl adList："+JSONArray.toJSON(adList));

        return adList;
    }
*/




    public List<Map<String, Object>> selectAd2(Map map){
        List<Map<String, Object>> adList = adMapper.selectAd(map);
        Map oMap = new HashMap();
        oMap.put("page", 0);
        oMap.put("rows", 10);
        List<Map<String, Object>> overallList = overallMapper.selectOverall(oMap);
        System.out.println("overallList："+overallList);

        for(Map<String, Object> adMap : adList){
            BigDecimal revenue = (BigDecimal)adMap.get("revenue");
            BigDecimal cost = (BigDecimal)adMap.get("cost");


            // ROAS
            boolean zero = cost.compareTo(BigDecimal.ZERO)==0;
//            System.out.println("zero:"+zero);

            BigDecimal roas = new BigDecimal(0.00);
            roas.setScale(2, BigDecimal.ROUND_HALF_UP);
            if(zero){
                adMap.put("roas", "0.00");
            }else{
                adMap.put("roas", revenue.divide(cost,2, RoundingMode.HALF_UP));
                roas = revenue.divide(cost, 2, RoundingMode.HALF_UP);
            }

            // 广告成本占比
            boolean zero2 = revenue.compareTo(BigDecimal.ZERO)==0;
            BigDecimal costProportion = new BigDecimal(0.00);
            costProportion.setScale(2, BigDecimal.ROUND_HALF_UP);
//            System.out.println("zero:"+zero);
            if(zero2){
                adMap.put("costProportion", "0");
//                costProportion = "0.00;
            }else{
                adMap.put("costProportion", cost.divide(revenue,2, RoundingMode.HALF_UP));
                costProportion = cost.divide(revenue,2, RoundingMode.HALF_UP);
            }


//            adMap.put("revenue", "$" + revenue + " (" + roas + "%" +  ")");
            adMap.put("revenue", "$" + revenue);
//            adMap.put("cost", "$" + cost + " (" + costProportion + "%" +  ")");
            adMap.put("cost", "$" + cost);


            if(adMap.get("logisticCostRatio") == null && adMap.get("goodsCostRatio") == null && adMap.get("refundRate") == null
                    && adMap.get("toolCostRatio") == null && adMap.get("passCostRatio") == null && adMap.get("operateCostRatio") == null){

                System.out.println("jump revenue："+revenue);


                if(overallList.size() > 0){
                    Map<String, Object> overallMap = overallList.get(0);
//                    String logisticCostRatio = (String)overallMap.getOrDefault("logisticCostRatio", 0);
//                    String goodsCostRatio = (String)overallMap.getOrDefault("goodsCostRatio", 0);
//                    String refundRate = (String)overallMap.getOrDefault("refundRate", 0);
//                    String toolCostRatio = (String)overallMap.getOrDefault("toolCostRatio", 0);
//                    String passCostRatio = (String)overallMap.getOrDefault("passCostRatio", 0);
//                    String operateCostRatio = (String)overallMap.getOrDefault("operateCostRatio", 0);

                    BigDecimal logisticCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("logisticCostRatio", 0)));// 物流成本占比
                    BigDecimal goodsCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("goodsCostRatio", 0))); // 商品成本占比
                    BigDecimal refundRate = new BigDecimal(String.valueOf(overallMap.getOrDefault("refundRate", 0))); // 退款率
                    BigDecimal toolCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("toolCostRatio", 0))); // 工具成本占比
                    BigDecimal passCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("passCostRatio", 0))); // 通道成本占比
                    BigDecimal operateCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("operateCostRatio", 0))); // 运营成本占比

/*
                    adMap.put("logisticCostRatio", logisticCostRatio.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                    adMap.put("goodsCostRatio", goodsCostRatio.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                    adMap.put("refundRate", refundRate.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                    adMap.put("toolCostRatio", toolCostRatio.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                    adMap.put("passCostRatio", passCostRatio.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                    adMap.put("operateCostRatio", operateCostRatio.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
*/

                    adMap.put("logisticCostRatio", 0);
                    adMap.put("goodsCostRatio", 0);
                    adMap.put("refundRate", 0);
                    adMap.put("toolCostRatio", 0);
                    adMap.put("passCostRatio", 0);
                    adMap.put("operateCostRatio", 0);

                }
//                continue;
            }






            System.out.println("++++++++++++++adMap:"+ JSONObject.toJSON(adMap));
//            BigDecimal operateCost = new BigDecimal(operateCostStr);// 运营成本

            BigDecimal operateCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("operateCostRatio", 0)));// 运营成本占比
            BigDecimal logisticCost = new BigDecimal(String.valueOf(adMap.getOrDefault("logisticCost", 0))); // 物流成本
            BigDecimal logisticCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("logisticCostRatio", 0))); // 物流成本占比
            BigDecimal goodsCost = new BigDecimal(String.valueOf(adMap.getOrDefault("goodsCost", 0))); // 商品成本
            BigDecimal goodsCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("goodsCostRatio", 0))); // 商品成本占比
            BigDecimal refund = new BigDecimal(String.valueOf(adMap.getOrDefault("refund", 0))); // 退款
            BigDecimal refundRate = new BigDecimal(String.valueOf(adMap.getOrDefault("refundRate",0))); // 退款率
            BigDecimal toolCost = new BigDecimal(String.valueOf(adMap.getOrDefault("toolCost", 0))); // 工具成本
            BigDecimal toolCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("toolCostRatio", 0))); // 工具成本占比
            BigDecimal passCost = new BigDecimal(String.valueOf(adMap.getOrDefault("passCost", 0))); // 通道成本
            BigDecimal passCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("passCostRatio", 0))); // 通道成本占比

            BigDecimal operateCost = revenue.multiply(operateCostRatio).setScale(2, BigDecimal.ROUND_HALF_UP);

            // 取全局
            boolean logisticCostRatioBoo = logisticCostRatio.compareTo(BigDecimal.ZERO)==0;
            boolean goodsCostRatioBoo = goodsCostRatio.compareTo(BigDecimal.ZERO)==0;
            boolean refundRateBoo = refundRate.compareTo(BigDecimal.ZERO)==0;
            boolean operateCostRatioBoo = operateCostRatio.compareTo(BigDecimal.ZERO)==0;
            boolean toolCostRatioBoo = toolCostRatio.compareTo(BigDecimal.ZERO)==0;
            boolean passCostRatioBoo = passCostRatio.compareTo(BigDecimal.ZERO)==0;

            if(logisticCostRatioBoo && goodsCostRatioBoo && refundRateBoo
                    && operateCostRatioBoo && toolCostRatioBoo && passCostRatioBoo){
                System.out.println("都是0.00");

                Map<String, Object> overallMap = overallList.get(0);

                System.out.println("if overallMap："+overallMap);

                logisticCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("logisticCostRatio", 0)));// 物流成本占比
                goodsCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("goodsCostRatio", 0))); // 商品成本占比
                refundRate = new BigDecimal(String.valueOf(overallMap.getOrDefault("refundRate", 0))); // 退款率
                toolCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("toolCostRatio", 0))); // 工具成本占比
                passCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("passCostRatio", 0))); // 通道成本占比
                operateCostRatio = new BigDecimal(String.valueOf(overallMap.getOrDefault("operateCostRatio", 0))); // 运营成本占比

                adMap.put("logisticCostRatio", roundHalfUp(logisticCostRatio));
                adMap.put("goodsCostRatio", roundHalfUp(goodsCostRatio));
                adMap.put("refundRate", roundHalfUp(refundRate));
                adMap.put("operateCostRatio", roundHalfUp(operateCostRatio));
                adMap.put("toolCostRatio", roundHalfUp(toolCostRatio));
                adMap.put("passCostRatio", roundHalfUp(passCostRatio));

                BigDecimal bd = new BigDecimal(1000);
                logisticCost = logisticCostRatio.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
                goodsCost = goodsCostRatio.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
                refundRate = refund.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
                toolCost = toolCostRatio.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
                passCost = passCostRatio.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
                operateCost = operateCostRatio.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);

            }


            // 运营成本

            adMap.put("operateCost", "$" + roundHalfUp(operateCost)); // 收入*运营成本占比

            // 利润
            // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
            BigDecimal profit = revenue.subtract(cost).subtract(logisticCost).subtract(goodsCost).subtract(operateCost).subtract(refund).subtract(toolCost).subtract(passCost);

            // 利润率
            // 利润/收入
            boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;
//            System.out.println("zero:"+zero);
            BigDecimal profitRate = new BigDecimal(0.00);
            if(revenueZero){
//                adMap.put("profitRate", "$" + "0");
            }else{
                profitRate = profit.divide(revenue, 2, BigDecimal.ROUND_HALF_UP);
//                adMap.put("profitRate", "$" + profitRate);
            }

//            adMap.put("profit", "$" + profit + " (" + profitRate + "%" +  ")");

//            adMap.put("profit", "$" + String.valueOf(profitValue));

            adMap.put("profitRate", roundHalfUp(profitRate));

            // 物流成本
//            adMap.put("logisticCost", "$" + logisticCost + " (" + logisticCostRatio + "%" +  ")");
            adMap.put("logisticCost", "$" + roundHalfUp(logisticCost));
            adMap.put("logisticCostRatio", roundHalfUp(logisticCostRatio));
            // 商品成本
//            adMap.put("goodsCost", "$" + goodsCost + " (" + goodsCostRatio + "%" +  ")");
            adMap.put("goodsCost", "$" + roundHalfUp(goodsCost));
            adMap.put("goodsCostRatio", roundHalfUp(goodsCostRatio));
            // 运营成本
//            adMap.put("operateCost", "$" + operateCost + " (" + operateCostRatio + "%" +  ")");
//            System.out.println("---------operateCostDouble1："+operateCost);
//            double operateCostDouble = operateCost.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
//            System.out.println("operateCostDouble2："+operateCostDouble);
            adMap.put("operateCost", "$" + roundHalfUp(operateCost));
            adMap.put("operateCostRatio", roundHalfUp(operateCostRatio));
            // 退款
//            adMap.put("refund", "$" + refund + " (" + refundRate + "%" +  ")");
            adMap.put("refund", "$" + roundHalfUp(refund));
            adMap.put("refundRate", roundHalfUp(refundRate));
            // 工具成本
//            adMap.put("toolCost", "$" + toolCost + " (" + toolCostRatio + "%" +  ")");
            adMap.put("toolCost", "$" + roundHalfUp(toolCost));
            adMap.put("toolCostRatio", roundHalfUp(toolCostRatio));
            // 通道成本
//            adMap.put("passCost", "$" + passCost + " (" + passCostRatio + "%" +  ")");
            adMap.put("passCost", "$" + roundHalfUp(passCost));
            adMap.put("passCostRatio", roundHalfUp(passCostRatio));

//            double profitValue = profit.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
            // 利润
//            adMap.put("profit", "$" + profit + " (" + profitRate + "%" +  ")");
            adMap.put("profit", "$" + roundHalfUp(profit));







            System.out.println("---------------adMap："+adMap);

        }

        System.out.println("adServuceImpl adList："+JSONArray.toJSON(adList));

        return adList;
    }

    public int selectAdCount(Map map){
        int adCount = adMapper.selectAdCount(map);
        return adCount;
    }


    public List<Map<String, Object>> selectAdByItemsId(@Param("itemsId") long itemsId){
        List<Map<String, Object>> adList = adMapper.selectAdByItemsId(itemsId);
        return adList;
    }

    public void insertAd(Map map){
        adMapper.insertAd(map);
    }

    public List<Map<String, Object>> selectAdAccountByItemsIdAndJobNumber(long itemsId, String jobNumber){
        List<Map<String, Object>> adAccountList = adMapper.selectAdAccountByItemsIdAndJobNumber(itemsId, jobNumber);
        return adAccountList;

    }

    public List<Map<String, Object>> selectAdItems(Map map){
        List<Map<String, Object>> adMap = adMapper.selectAdItems(map);

        return adMap;
    }


    public Map<String, Object> selectAllSum(List<Map<String, Object>> adList, Map map){
        System.out.println("开始汇总...");
//        Map<String, Object> adMap = adMapper.selectAllSum(map);

        map.put("page", 0);
        map.put("rows", 1000);


        BigDecimal revenue = new BigDecimal(0);
        BigDecimal roas = new BigDecimal(0);
        BigDecimal cost = new BigDecimal(0);
        BigDecimal costProportion = new BigDecimal(0);
        BigDecimal logisticCost = new BigDecimal(0);
        BigDecimal logisticCostRatio = new BigDecimal(0);
        BigDecimal goodsCost = new BigDecimal(0);
        BigDecimal goodsCostRatio = new BigDecimal(0);
        BigDecimal operateCost = new BigDecimal(0);
        BigDecimal operateCostRatio = new BigDecimal(0);
        BigDecimal refund = new BigDecimal(0);
        BigDecimal refundRate = new BigDecimal(0);


        BigDecimal toolCost = new BigDecimal(0);
        BigDecimal toolCostRatio = new BigDecimal(0);
        BigDecimal passCost = new BigDecimal(0);
        BigDecimal passCostRatio = new BigDecimal(0);
        BigDecimal profit = new BigDecimal(0);
        BigDecimal profitRate = new BigDecimal(0);

//        List<Map<String, Object>> adList = selectAd(map);
        System.out.println("///////////////////////adList："+adList);
        for(Map<String, Object> adMap : adList){
//            System.out.println("********adMap："+JSONObject.toJSON(adMap));
            revenue = revenue.add((BigDecimal)adMap.get("revenue"));
//            roas = roas.add((BigDecimal)adMap.get("roas"));
            cost = cost.add((BigDecimal)adMap.get("cost"));


            BigDecimal change = new BigDecimal(100);
            BigDecimal init = new BigDecimal(0.00);

            // ROAS
            boolean costZero = cost.compareTo(BigDecimal.ZERO)==0;
            boolean isRevenue = revenue.compareTo(BigDecimal.ZERO)==0;
            roas = costZero ? init : revenue.divide(cost, 2, RoundingMode.HALF_UP);

            map.put("revenue", "$" + revenue + "(" + roas +  ")");
            map.put("roas", roas);
            costProportion = isRevenue? change : (cost.divide(revenue, 4, RoundingMode.HALF_UP)).multiply(change).setScale(2, RoundingMode.HALF_UP);

            map.put("cost", "$" + roundHalfUp(cost) + "(" + costProportion + "%" +  ")");
            map.put("costProportion", costProportion);


            if(adMap.get("logisticCost") != null && adMap.get("goodsCost") != null && adMap.get("operateCost") != null
                    && adMap.get("refund") != null&& adMap.get("toolCost") != null&& adMap.get("passCost") != null){
                //            costProportion = costProportion.add((BigDecimal)adMap.get("costProportion"));
                logisticCost = logisticCost.add((BigDecimal)adMap.get("logisticCost"));
//            logisticCostRatio = logisticCostRatio.add((BigDecimal)adMap.get("logisticCostRatio"));
                goodsCost = goodsCost.add((BigDecimal)adMap.get("goodsCost"));
//            goodsCostRatio = goodsCostRatio.add((BigDecimal)adMap.get("goodsCostRatio"));
                operateCost = operateCost.add((BigDecimal)adMap.get("operateCost"));
//            operateCostRatio = operateCostRatio.add((BigDecimal)adMap.get("operateCostRatio"));
                refund = refund.add((BigDecimal)adMap.get("refund"));

//            refundRate = refundRate.add((BigDecimal)adMap.get("refundRate"));
                toolCost = toolCost.add((BigDecimal)adMap.get("toolCost"));
//            toolCostRatio = toolCostRatio.add((BigDecimal)adMap.get("toolCostRatio"));
                passCost = passCost.add((BigDecimal)adMap.get("passCost"));
//            passCostRatio = passCostRatio.add((BigDecimal)adMap.get("passCostRatio"));

                profit = profit.add((BigDecimal)adMap.get("profit"));
                profitRate = profitRate.add((BigDecimal)adMap.get("profitRate"));
            }






            logisticCostRatio = isRevenue? init : logisticCost.divide(revenue, 4, BigDecimal.ROUND_HALF_UP);
            logisticCostRatio = logisticCostRatio.multiply(change).setScale(2, BigDecimal.ROUND_DOWN);
            map.put("logisticCost", "$" + roundHalfUp(logisticCost) + "(" + logisticCostRatio + "%" +  ")");
//            BigDecimal refund = roundHalfUp(revenue.multiply(refundRate.divide(change, 4, BigDecimal.ROUND_HALF_UP)));
            map.put("logisticCostRatio", logisticCostRatio);

            goodsCostRatio = isRevenue? init : goodsCost.divide(revenue, 4, BigDecimal.ROUND_HALF_UP);
            goodsCostRatio = goodsCostRatio.multiply(change).setScale(2, BigDecimal.ROUND_DOWN);
            map.put("goodsCost", "$" + roundHalfUp(goodsCost) + "(" + goodsCostRatio + "%" +  ")");
            map.put("goodsCostRatio", goodsCostRatio);

            operateCostRatio = isRevenue? init : operateCost.divide(revenue, 4, BigDecimal.ROUND_HALF_UP);
            operateCostRatio = operateCostRatio.multiply(change).setScale(2, BigDecimal.ROUND_DOWN);
            map.put("operateCost", "$" + roundHalfUp(operateCost) + "(" + operateCostRatio + "%" +  ")");
            map.put("operateCostRatio", operateCostRatio);
            refundRate = isRevenue? init : refund.divide(revenue, 4, BigDecimal.ROUND_HALF_UP);
            refundRate = refundRate.multiply(change).setScale(2, BigDecimal.ROUND_DOWN);
            map.put("refund", "$" + roundHalfUp(refund) + "(" + refundRate + "%" +  ")");
            map.put("refundRate", refundRate);
            toolCostRatio = isRevenue? init : toolCost.divide(revenue, 4, BigDecimal.ROUND_HALF_UP);
            toolCostRatio = toolCostRatio.multiply(change).setScale(2, BigDecimal.ROUND_DOWN);
            map.put("toolCost", "$" + roundHalfUp(toolCost) + "(" + toolCostRatio + "%" +  ")");
            map.put("toolCostRatio", toolCostRatio);
            passCostRatio = isRevenue? init : passCost.divide(revenue, 4, BigDecimal.ROUND_HALF_UP);
            passCostRatio = passCostRatio.multiply(change).setScale(2, BigDecimal.ROUND_DOWN);
            map.put("passCost", "$" + roundHalfUp(passCost) + "(" + passCostRatio + "%" +  ")");
            map.put("passCostRatio", passCostRatio);

            profitRate = isRevenue? init : profit.divide(revenue, 4, BigDecimal.ROUND_HALF_UP);
            profitRate = profitRate.multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP);



            map.put("profit", "$" + roundHalfUp(profit) + "(" + profitRate + "%" +  ")");
            map.put("profitRate", profitRate);

        }

        System.out.println("777map："+map);

        return map;
    }


    public Map<String, Object> selectChannelAllSum(List<Map<String, Object>> adList, Map map){
        System.out.println("开始汇总...");
//        Map<String, Object> adMap = adMapper.selectAllSum(map);

//        Map<String, Object> adMap = new HashMap<>();

        map.put("page", 0);
        map.put("rows", 1000);


        BigDecimal revenue = new BigDecimal(0);
        BigDecimal roas = new BigDecimal(0);
        BigDecimal cost = new BigDecimal(0);
        BigDecimal costProportion = new BigDecimal(0);
        BigDecimal logisticCost = new BigDecimal(0);
        BigDecimal logisticCostRatio = new BigDecimal(0);
        BigDecimal goodsCost = new BigDecimal(0);
        BigDecimal goodsCostRatio = new BigDecimal(0);
        BigDecimal operateCost = new BigDecimal(0);
        BigDecimal operateCostRatio = new BigDecimal(0);
        BigDecimal refund = new BigDecimal(0);
        BigDecimal refundRate = new BigDecimal(0);


        BigDecimal toolCost = new BigDecimal(0);
        BigDecimal toolCostRatio = new BigDecimal(0);
        BigDecimal passCost = new BigDecimal(0);
        BigDecimal passCostRatio = new BigDecimal(0);
        BigDecimal profit = new BigDecimal(0);
        BigDecimal profitRate = new BigDecimal(0);

//        List<Map<String, Object>> adList = selectAd(map);
        System.out.println("///////////////////////adList："+adList);
        for(Map<String, Object> adMap : adList){
            revenue = revenue.add((BigDecimal)adMap.get("revenue"));
            roas = roas.add((BigDecimal)adMap.get("roas"));
            cost = cost.add((BigDecimal)adMap.get("cost"));
            costProportion = costProportion.add((BigDecimal)adMap.get("costProportion"));
            logisticCost = logisticCost.add((BigDecimal)adMap.get("logisticCost"));
            logisticCostRatio = logisticCostRatio.add((BigDecimal)adMap.get("logisticCostRatio"));
            goodsCost = goodsCost.add((BigDecimal)adMap.get("goodsCost"));
            goodsCostRatio = goodsCostRatio.add((BigDecimal)adMap.get("goodsCostRatio"));
            operateCost = operateCost.add((BigDecimal)adMap.get("operateCost"));
            operateCostRatio = operateCostRatio.add((BigDecimal)adMap.get("operateCostRatio"));
            refund = refund.add((BigDecimal)adMap.get("refund"));

            refundRate = refundRate.add((BigDecimal)adMap.get("refundRate"));
            toolCost = toolCost.add((BigDecimal)adMap.get("toolCost"));
            toolCostRatio = toolCostRatio.add((BigDecimal)adMap.get("toolCostRatio"));
            passCost = passCost.add((BigDecimal)adMap.get("passCost"));
            passCostRatio = passCostRatio.add((BigDecimal)adMap.get("passCostRatio"));
            profit = profit.add((BigDecimal)adMap.get("profit"));
            profitRate = profitRate.add((BigDecimal)adMap.get("profitRate"));

            map.put("revenue", revenue + " (" + roas + "%" +  ")");
            map.put("roas", roas);
            map.put("cost", cost + " (" + costProportion + "%" +  ")");
            map.put("costProportion", costProportion);
            map.put("logisticCost", logisticCost + " (" + logisticCostRatio + "%" +  ")");
            map.put("logisticCostRatio", logisticCostRatio);
            map.put("goodsCost", goodsCost + " (" + goodsCostRatio + "%" +  ")");
            map.put("goodsCostRatio", goodsCostRatio);
            map.put("operateCost", operateCost + " (" + operateCostRatio + "%" +  ")");
            map.put("operateCostRatio", operateCostRatio);
            map.put("refund", refund + " (" + refundRate + "%" +  ")");
            map.put("refundRate", refundRate);
            map.put("toolCost", toolCost + " (" + toolCostRatio + "%" +  ")");
            map.put("toolCostRatio", toolCostRatio);
            map.put("passCost", passCost + " (" + passCostRatio + "%" +  ")");
            map.put("passCostRatio", passCostRatio);
            map.put("profit", profit + " (" + profitRate + "%" +  ")");
            map.put("profitRate", profitRate);

        }




        return map;
    }


    public Map<String, Object> selectAllSum3(Map map){
        System.out.println("开始汇总...");
        Map<String, Object> adMap = adMapper.selectAllSum(map);

        System.out.println("adMap："+JSONObject.toJSON(adMap));


        BigDecimal revenue = (BigDecimal)adMap.get("revenue");
        BigDecimal cost = (BigDecimal)adMap.get("cost");


        // ROAS
        boolean zero = cost.compareTo(BigDecimal.ZERO)==0;
//            System.out.println("zero:"+zero);

        BigDecimal roas = new BigDecimal(0.00);
        roas.setScale(2, BigDecimal.ROUND_HALF_UP);
        if(zero){
            adMap.put("roas", "0.00");
        }else{
            adMap.put("roas", revenue.divide(cost,2, RoundingMode.HALF_UP));
            roas = revenue.divide(cost, 2, RoundingMode.HALF_UP);
        }

        // 广告成本占比
        boolean zero2 = revenue.compareTo(BigDecimal.ZERO)==0;
        BigDecimal costProportion = new BigDecimal(0.00);
        costProportion.setScale(2, BigDecimal.ROUND_HALF_UP);
//            System.out.println("zero:"+zero);
        if(zero2){
            adMap.put("costProportion", "0");
//                costProportion = "0.00;
        }else{
            adMap.put("costProportion", cost.divide(revenue,2, RoundingMode.HALF_UP));
            costProportion = cost.divide(revenue,2, RoundingMode.HALF_UP);
        }


//            adMap.put("revenue", "$" + revenue + " (" + roas + "%" +  ")");
        adMap.put("revenue", "$" + revenue + " (" + roas +  ")");
//            adMap.put("cost", "$" + cost + " (" + costProportion + "%" +  ")");
        adMap.put("cost", "$" + cost + " (" + costProportion + "%" +  ")");



        if(adMap.get("logisticCostRatio") == null && adMap.get("goodsCostRatio") == null && adMap.get("refundRate") == null
                && adMap.get("toolCostRatio") == null && adMap.get("passCostRatio") == null && adMap.get("operateCostRatio") == null){

            System.out.println("jump revenue："+revenue);


//                continue;
        }

        System.out.println("++++++++++++++adMap:"+ JSONObject.toJSON(adMap));
//            BigDecimal operateCost = new BigDecimal(operateCostStr);// 运营成本

        BigDecimal operateCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("operateCostRatio", 0)));// 运营成本占比
        BigDecimal logisticCost = new BigDecimal(String.valueOf(adMap.getOrDefault("logisticCost", 0))); // 物流成本
        BigDecimal logisticCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("logisticCostRatio", 0))); // 物流成本占比
        BigDecimal goodsCost = new BigDecimal(String.valueOf(adMap.getOrDefault("goodsCost", 0))); // 商品成本
        BigDecimal goodsCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("goodsCostRatio", 0))); // 商品成本占比
        BigDecimal refund = new BigDecimal(String.valueOf(adMap.getOrDefault("refund", 0))); // 退款
        BigDecimal refundRate = new BigDecimal(String.valueOf(adMap.getOrDefault("refundRate",0))); // 退款率
        BigDecimal toolCost = new BigDecimal(String.valueOf(adMap.getOrDefault("toolCost", 0))); // 工具成本
        BigDecimal toolCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("toolCostRatio", 0))); // 工具成本占比
        BigDecimal passCost = new BigDecimal(String.valueOf(adMap.getOrDefault("passCost", 0))); // 通道成本
        BigDecimal passCostRatio = new BigDecimal(String.valueOf(adMap.getOrDefault("passCostRatio", 0))); // 通道成本占比

        BigDecimal operateCost = revenue.multiply(operateCostRatio).setScale(2, BigDecimal.ROUND_HALF_UP);

        // 取全局
        boolean logisticCostRatioBoo = logisticCostRatio.compareTo(BigDecimal.ZERO)==0;
        boolean goodsCostRatioBoo = goodsCostRatio.compareTo(BigDecimal.ZERO)==0;
        boolean refundRateBoo = refundRate.compareTo(BigDecimal.ZERO)==0;
        boolean operateCostRatioBoo = operateCostRatio.compareTo(BigDecimal.ZERO)==0;
        boolean toolCostRatioBoo = toolCostRatio.compareTo(BigDecimal.ZERO)==0;
        boolean passCostRatioBoo = passCostRatio.compareTo(BigDecimal.ZERO)==0;

//        if(logisticCostRatioBoo && goodsCostRatioBoo && refundRateBoo
//                && operateCostRatioBoo && toolCostRatioBoo && passCostRatioBoo){
        System.out.println("都是0.00-----------");

//                Map<String, Object> overallMap = overallList.get(0);
        Map<String, Object> overallMap = adMap;

        System.out.println("if overallMap："+overallMap);

        logisticCostRatio = logisticCostRatio.add(new BigDecimal(String.valueOf(overallMap.getOrDefault("logisticCostRatioAll", 0))));// 物流成本占比
        goodsCostRatio = logisticCostRatio.add(new BigDecimal(String.valueOf(overallMap.getOrDefault("goodsCostRatioAll", 0)))); // 商品成本占比
        refundRate = logisticCostRatio.add(new BigDecimal(String.valueOf(overallMap.getOrDefault("refundRateAll", 0)))); // 退款率
        toolCostRatio = logisticCostRatio.add(new BigDecimal(String.valueOf(overallMap.getOrDefault("toolCostRatioAll", 0)))); // 工具成本占比
        passCostRatio = logisticCostRatio.add(new BigDecimal(String.valueOf(overallMap.getOrDefault("passCostRatioAll", 0)))); // 通道成本占比
        operateCostRatio = logisticCostRatio.add(new BigDecimal(String.valueOf(overallMap.getOrDefault("operateCostRatioAll", 0)))); // 运营成本占比

        logisticCost = logisticCostRatio.add(new BigDecimal(String.valueOf(overallMap.getOrDefault("logisticCostAll", 0))));// 物流成本占比
        goodsCost = logisticCostRatio.add(new BigDecimal(String.valueOf(overallMap.getOrDefault("goodsCostAll", 0)))); // 商品成本占比
        refund = logisticCostRatio.add(new BigDecimal(String.valueOf(overallMap.getOrDefault("refundAll", 0)))); // 退款率
        toolCost = logisticCostRatio.add(new BigDecimal(String.valueOf(overallMap.getOrDefault("toolCostAll", 0)))); // 工具成本占比
        passCost = logisticCostRatio.add(new BigDecimal(String.valueOf(overallMap.getOrDefault("passCostAll", 0)))); // 通道成本占比
        operateCost = logisticCostRatio.add(new BigDecimal(String.valueOf(overallMap.getOrDefault("operateCostAll", 0)))); // 运营成本占比


        adMap.put("logisticCostRatio", roundHalfUp(logisticCostRatio));
        adMap.put("goodsCostRatio", roundHalfUp(goodsCostRatio));
        adMap.put("refundRate", roundHalfUp(refundRate));
        adMap.put("operateCostRatio", roundHalfUp(operateCostRatio));
        adMap.put("toolCostRatio", roundHalfUp(toolCostRatio));
        adMap.put("passCostRatio", roundHalfUp(passCostRatio));

        adMap.put("logisticCost", roundHalfUp(logisticCost));
        adMap.put("goodsCost", roundHalfUp(goodsCost));
        adMap.put("refund", roundHalfUp(refund));
        adMap.put("operateCost", roundHalfUp(operateCost));
        adMap.put("toolCost", roundHalfUp(toolCost));
        adMap.put("passCost", roundHalfUp(passCost));

        System.out.println("adMap："+adMap);

        System.out.println("logisticCostRatio："+logisticCostRatio);
        System.out.println("revenue："+revenue);
        System.out.println("logisticCost："+logisticCost);
        System.out.println("-------------------------");


//                logisticCostRatio = logisticCostRatio.divide(100,2, RoundingMode.HALF_UP));
        BigDecimal convert = new BigDecimal(100);
        BigDecimal logisticCostRatioConvert = logisticCostRatio.divide(convert,2, RoundingMode.HALF_UP);
        BigDecimal goodsCostRatioConvert = goodsCostRatio.divide(convert,2, RoundingMode.HALF_UP);
        BigDecimal refundRateConvert = refundRate.divide(convert,2, RoundingMode.HALF_UP);
        BigDecimal toolCostRatioConvert = toolCostRatio.divide(convert,2, RoundingMode.HALF_UP);
        BigDecimal passCostRatioConvert = passCostRatio.divide(convert,2, RoundingMode.HALF_UP);
        BigDecimal operateCostRatioConvert = operateCostRatio.divide(convert,2, RoundingMode.HALF_UP);


//                BigDecimal bd = new BigDecimal(1000);
        BigDecimal bd = revenue;
        logisticCost = logisticCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
        goodsCost = goodsCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
        refund = refundRateConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
        toolCost = toolCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
        passCost = passCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
        operateCost = operateCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);


        System.out.println("logisticCost2："+logisticCost);
        System.out.println("-------------------------");

//        }


        // 运营成本

        adMap.put("operateCost", "$" + roundHalfUp(operateCost)); // 收入*运营成本占比

        // 利润
        // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
        BigDecimal profit = revenue.subtract(cost).subtract(logisticCost).subtract(goodsCost).subtract(operateCost).subtract(refund).subtract(toolCost).subtract(passCost);

        // 利润率
        // 利润/收入
        boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;
//            System.out.println("zero:"+zero);
        BigDecimal profitRate = new BigDecimal(0.00);
        if(revenueZero){
//                adMap.put("profitRate", "$" + "0");
        }else{
            profitRate = profit.divide(revenue, 2, BigDecimal.ROUND_HALF_UP);
//                adMap.put("profitRate", "$" + profitRate);
        }



        // 物流成本
//            adMap.put("logisticCost", "$" + logisticCost + " (" + logisticCostRatio + "%" +  ")");
        adMap.put("logisticCost", "$" + roundHalfUp(logisticCost) + " (" +  roundHalfUp(logisticCostRatio) + "%" +  ")");
//        adMap.put("logisticCostRatio", roundHalfUp(logisticCostRatio));
        // 商品成本
//            adMap.put("goodsCost", "$" + goodsCost + " (" + goodsCostRatio + "%" +  ")");
        adMap.put("goodsCost", "$" + roundHalfUp(goodsCost) + " (" +  roundHalfUp(goodsCostRatio) + "%" +  ")");
//        adMap.put("goodsCostRatio", roundHalfUp(goodsCostRatio));
        // 运营成本
//            adMap.put("operateCost", "$" + operateCost + " (" + operateCostRatio + "%" +  ")");
//            System.out.println("---------operateCostDouble1："+operateCost);
//            double operateCostDouble = operateCost.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
//            System.out.println("operateCostDouble2："+operateCostDouble);
        adMap.put("operateCost", "$" + roundHalfUp(operateCost) + " (" +  roundHalfUp(operateCostRatio) + "%" +  ")");
//        adMap.put("operateCostRatio", roundHalfUp(operateCostRatio));
        // 退款
//            adMap.put("refund", "$" + refund + " (" + refundRate + "%" +  ")");
        adMap.put("refund", "$" + roundHalfUp(refund) + " (" +  roundHalfUp(refundRate) + "%" +  ")");
//        adMap.put("refundRate", roundHalfUp(refundRate));
        // 工具成本
//            adMap.put("toolCost", "$" + toolCost + " (" + toolCostRatio + "%" +  ")");
        adMap.put("toolCost", "$" + roundHalfUp(toolCost) + " (" +  roundHalfUp(toolCostRatio) + "%" +  ")");
//        adMap.put("toolCostRatio", roundHalfUp(toolCostRatio));
        // 通道成本
//            adMap.put("passCost", "$" + passCost + " (" + passCostRatio + "%" +  ")");
        adMap.put("passCost", "$" + roundHalfUp(passCost) + " (" +  roundHalfUp(passCostRatio) + "%" +  ")");
//        adMap.put("passCostRatio", roundHalfUp(passCostRatio));

//            double profitValue = profit.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        // 利润
//            adMap.put("profit", "$" + profit + " (" + profitRate + "%" +  ")");
        adMap.put("profit", "$" + roundHalfUp(profit) + " (" +  roundHalfUp(profitRate) + "%" +  ")");
//        adMap.put("profitRate", roundHalfUp(profitRate));



/*

        adSumMap.put("profit", "$" +  roundHalfUp(profit) + " (" +  roundHalfUp(profitRate) + "%" +  ")");

        // 物流成本
        adSumMap.put("logisticCost", "$" +  roundHalfUp(logisticCost) + " (" +  roundHalfUp(logisticCostRatio) + "%" +  ")");
        // 商品成本
        adSumMap.put("goodsCost", "$" +  roundHalfUp(goodsCost) + " (" +  roundHalfUp(goodsCostRatio) + "%" +  ")");
        // 运营成本
        adSumMap.put("operateCost", "$" +  roundHalfUp(operateCost) + " (" +  roundHalfUp(operateCostRatio) + "%" +  ")");
        // 退款
        adSumMap.put("refund", "$" +  roundHalfUp(refund) + " (" +  roundHalfUp(refundRate) + "%" +  ")");
        // 工具成本
        adSumMap.put("toolCost", "$" +  roundHalfUp(toolCost) + " (" +  roundHalfUp(toolCostRatio) + "%" +  ")");
        // 通道成本
        adSumMap.put("passCost", "$" +  roundHalfUp(passCost) + " (" +  roundHalfUp(passCostRatio) + "%" +  ")");
        // 利润
        adSumMap.put("profit", "$" + roundHalfUp(profit) + " (" + roundHalfUp(profitRate) + "%" +  ")");
*/

        System.out.println("---------------adMap："+adMap);

        System.out.println("++++++++++++adSumMap："+JSONObject.toJSON(adMap));
        return adMap;
    }


    public Map<String, Object> selectAllSum2(Map map){
        Map<String, Object> adSumMap = adMapper.selectAllSum(map);

        BigDecimal revenueSum = (BigDecimal)adSumMap.get("revenue");
        BigDecimal costSum = (BigDecimal)adSumMap.get("cost");

//        Map sumMap = new HashMap();

        BigDecimal roas = new BigDecimal(0.00);
        roas.setScale(2, RoundingMode.DOWN);
        // ROAS
        boolean zero = costSum.compareTo(BigDecimal.ZERO)==0;
//            System.out.println("zero:"+zero);
        if(zero){
            adSumMap.put("roas", "0.00");
        }else{
            adSumMap.put("roas", revenueSum.divide(costSum,2, RoundingMode.DOWN));
            roas = revenueSum.divide(costSum, 2, RoundingMode.DOWN);
        }

        // 广告成本占比
        boolean zero2 = revenueSum.compareTo(BigDecimal.ZERO)==0;
        BigDecimal costProportion = new BigDecimal(0.00);
        costProportion.setScale(2, RoundingMode.DOWN);
        if(zero2){
            adSumMap.put("costProportion", "0.00");
        }else{
            adSumMap.put("costProportion", costSum.divide(revenueSum,2, RoundingMode.DOWN));
            costProportion = costSum.divide(revenueSum,2, RoundingMode.DOWN);
        }



//        adSumMap.put("itemsName", "全部页汇总");
//            sumMap.put("revenue", "$" + revenueSum);
//            sumMap.put("cost", "$" + revenueSum);
//            sumMap.put("revenue", "$" + revenueSum + " (" + roas + "%" +  ")");
        adSumMap.put("revenue", "$" +  roundHalfUp(revenueSum) + " (" + roas + "%" +  ")");
        adSumMap.put("cost", "$" +  roundHalfUp(costSum) + " (" + costProportion + "%" +  ")");

//        if(adSumMap.get("operateCostRatio") != null && adSumMap.get("logisticCost") != null){

            BigDecimal operateCostRatio = new BigDecimal(String.valueOf(adSumMap.getOrDefault("operateCostRatio", 0)));// 运营成本占比
            BigDecimal logisticCost = new BigDecimal(String.valueOf(adSumMap.getOrDefault("logisticCost", 0))); // 物流成本
            BigDecimal logisticCostRatio = new BigDecimal(String.valueOf(adSumMap.getOrDefault("logisticCostRatio", 0))); // 物流成本占比
            BigDecimal goodsCost = new BigDecimal(String.valueOf(adSumMap.getOrDefault("goodsCost", 0))); // 商品成本
            BigDecimal goodsCostRatio = new BigDecimal(String.valueOf(adSumMap.getOrDefault("goodsCostRatio", 0))); // 商品成本占比
            BigDecimal refund = new BigDecimal(String.valueOf(adSumMap.getOrDefault("refund", 0))); // 退款
            BigDecimal refundRate = new BigDecimal(String.valueOf(adSumMap.getOrDefault("refundRate", 0))); // 退款率
            BigDecimal toolCost = new BigDecimal(String.valueOf(adSumMap.getOrDefault("toolCost", 0))); // 工具成本
            BigDecimal toolCostRatio = new BigDecimal(String.valueOf(adSumMap.getOrDefault("toolCostRatio", 0))); // 工具成本占比
            BigDecimal passCost = new BigDecimal(String.valueOf(adSumMap.getOrDefault("passCost", 0))); // 通道成本
            BigDecimal passCostRatio = new BigDecimal(String.valueOf(adSumMap.getOrDefault("passCostRatio", 0))); // 通道成本占比


            // 运营成本
            BigDecimal operateCost = revenueSum.multiply(operateCostRatio);
            adSumMap.put("operateCost", "$" + operateCost); // 收入*运营成本占比

            // 利润
            // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
            BigDecimal profit = revenueSum.subtract(costSum).subtract(logisticCost).subtract(goodsCost).subtract(operateCost).subtract(refund).subtract(toolCost).subtract(passCost);

            // 利润率
            // 利润/收入
            boolean revenueZero = revenueSum.compareTo(BigDecimal.ZERO)==0;
//            System.out.println("zero:"+zero);
            BigDecimal profitRate = new BigDecimal(0.00);
            if(revenueZero){
//                adMap.put("profitRate", "$" + "0");
            }else{
                profitRate = profit.divide(revenueSum, 2, RoundingMode.DOWN);
//                adMap.put("profitRate", "$" + profitRate);
            }

            adSumMap.put("profit", "$" +  roundHalfUp(profit) + " (" +  roundHalfUp(profitRate) + "%" +  ")");

            // 物流成本
            adSumMap.put("logisticCost", "$" +  roundHalfUp(logisticCost) + " (" +  roundHalfUp(logisticCostRatio) + "%" +  ")");
            // 商品成本
            adSumMap.put("goodsCost", "$" +  roundHalfUp(goodsCost) + " (" +  roundHalfUp(goodsCostRatio) + "%" +  ")");
            // 运营成本
            adSumMap.put("operateCost", "$" +  roundHalfUp(operateCost) + " (" +  roundHalfUp(operateCostRatio) + "%" +  ")");
            // 退款
            adSumMap.put("refund", "$" +  roundHalfUp(refund) + " (" +  roundHalfUp(refundRate) + "%" +  ")");
            // 工具成本
            adSumMap.put("toolCost", "$" +  roundHalfUp(toolCost) + " (" +  roundHalfUp(toolCostRatio) + "%" +  ")");
            // 通道成本
            adSumMap.put("passCost", "$" +  roundHalfUp(passCost) + " (" +  roundHalfUp(passCostRatio) + "%" +  ")");
            // 利润
            adSumMap.put("profit", "$" + roundHalfUp(profit) + " (" + roundHalfUp(profitRate) + "%" +  ")");

//        }

        System.out.println("++++++++++++adSumMap："+JSONObject.toJSON(adSumMap));
        return adSumMap;
    }



    public void insertAds(Map map){
        adMapper.insertAds(map);
    }

    public int updateAds(Map map){

        int count = adMapper.updateAds(map);
        Long items_id = (Long)map.get("items_id");
        System.out.println("count:"+count);
        System.out.println("items_id:"+items_id);
        return count;

    }

    public List<Map<String, Object>> selectAdsByItemsId(long itemsId){
        List<Map<String, Object>> adsList = adMapper.selectAdsByItemsId(itemsId);
        return adsList;
    }

    public List<Map<String, Object>> selectAdGroupByJobNumber(long itemsId, String jobNumber){
        List<Map<String, Object>> adsList = adMapper.selectAdGroupByJobNumber(itemsId, jobNumber);
        return adsList;
    }

    public List<Map<String, Object>> selectAdAccountGroupByItemsId(long itemsId){
        List<Map<String, Object>> adsList = adMapper.selectAdAccountGroupByItemsId(itemsId);
        return adsList;
    }

    public List<Map<String, Object>> selectAdGroupByAdAccount(long itemsId, String jobNumber){
        List<Map<String, Object>> adList = adMapper.selectAdGroupByAdAccount(itemsId, jobNumber);
        return adList;
    }

    public List<Map<String, Object>> selectAdChannel(Map map){
        List<Map<String, Object>> adList = adMapper.selectAdChannel(map);

        // 0525

        BigDecimal change = new BigDecimal(100);
        for(Map<String, Object> adMap : adList){
            BigDecimal revenue = (BigDecimal)adMap.get("revenue");
            BigDecimal cost = (BigDecimal)adMap.get("cost");


            // ROAS
            boolean zero = cost.compareTo(BigDecimal.ZERO)==0;
//            System.out.println("zero:"+zero);

            BigDecimal roas = new BigDecimal(0.00);
            roas.setScale(2, BigDecimal.ROUND_HALF_UP);
            if(zero){
                adMap.put("roas", roas);
            }else{
                adMap.put("roas", revenue.divide(cost,2, RoundingMode.HALF_UP));
                roas = revenue.divide(cost, 2, RoundingMode.HALF_UP);
            }

            // 广告成本占比
            boolean zero2 = revenue.compareTo(BigDecimal.ZERO)==0;
            BigDecimal costProportion = new BigDecimal(0.00);
            costProportion.setScale(4, BigDecimal.ROUND_HALF_UP);
//            System.out.println("zero:"+zero);
            if(zero2){
                adMap.put("costProportion", costProportion);
//                costProportion = "0.00;
            }else{
                adMap.put("costProportion", (cost.divide(revenue,4, RoundingMode.HALF_UP)).multiply(change));
                costProportion = cost.divide(revenue,2, RoundingMode.HALF_UP);
            }


//            adMap.put("revenue", "$" + revenue + " (" + roas + "%" +  ")");
            adMap.put("revenue", roundHalfUp(revenue));
//            adMap.put("cost", "$" + cost + " (" + costProportion + "%" +  ")");
            adMap.put("cost", roundHalfUp(cost));


//            BigDecimal operateCost = new BigDecimal(operateCostStr);// 运营成本
            BigDecimal operateCostRatio = new BigDecimal(String.valueOf(adMap.get("operateCostRatio")));// 运营成本占比
            BigDecimal logisticCost = new BigDecimal(String.valueOf(adMap.get("logisticCost"))); // 物流成本
            BigDecimal logisticCostRatio = new BigDecimal(String.valueOf(adMap.get("logisticCostRatio"))); // 物流成本占比
            BigDecimal goodsCost = new BigDecimal(String.valueOf(adMap.get("goodsCost"))); // 商品成本
            BigDecimal goodsCostRatio = new BigDecimal(String.valueOf(adMap.get("goodsCostRatio"))); // 商品成本占比
            BigDecimal refund = new BigDecimal(String.valueOf(adMap.get("refund"))); // 退款
            BigDecimal refundRate = new BigDecimal(String.valueOf(adMap.get("refundRate"))); // 退款率
            BigDecimal toolCost = new BigDecimal(String.valueOf(adMap.get("toolCost"))); // 工具成本
            BigDecimal toolCostRatio = new BigDecimal(String.valueOf(adMap.get("toolCostRatio"))); // 工具成本占比
            BigDecimal passCost = new BigDecimal(String.valueOf(adMap.get("passCost"))); // 通道成本
            BigDecimal passCostRatio = new BigDecimal(String.valueOf(adMap.get("passCostRatio"))); // 通道成本占比
            BigDecimal operateCost = new BigDecimal(String.valueOf(adMap.getOrDefault("operateCostAll", 0))); // 运营成本占比

//            Double operateCostRatioDouble = roundHalfUp(operateCostRatio);
            operateCostRatio = roundHalfUp(operateCostRatio);

            adMap.put("operateCostRatio", roundHalfUp(operateCostRatio));
            adMap.put("logisticCostRatio", roundHalfUp(logisticCostRatio));
            adMap.put("goodsCostRatio", roundHalfUp(goodsCostRatio));
            adMap.put("refundRate", roundHalfUp(refundRate));
            adMap.put("toolCostRatio", roundHalfUp(toolCostRatio));
            adMap.put("passCostRatio", roundHalfUp(passCostRatio));


            BigDecimal convert = new BigDecimal(100);
            BigDecimal logisticCostRatioConvert = logisticCostRatio.divide(convert,2, RoundingMode.HALF_UP);
            BigDecimal goodsCostRatioConvert = goodsCostRatio.divide(convert,2, RoundingMode.HALF_UP);
            BigDecimal refundRateConvert = refundRate.divide(convert,2, RoundingMode.HALF_UP);
            BigDecimal toolCostRatioConvert = toolCostRatio.divide(convert,2, RoundingMode.HALF_UP);
            BigDecimal passCostRatioConvert = passCostRatio.divide(convert,2, RoundingMode.HALF_UP);
            BigDecimal operateCostRatioConvert = operateCostRatio.divide(convert,2, RoundingMode.HALF_UP);

//                BigDecimal bd = new BigDecimal(1000);
            BigDecimal bd = revenue;
            logisticCost = logisticCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
            goodsCost = goodsCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
            refund = refundRateConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
            toolCost = toolCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
            passCost = passCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);
            operateCost = operateCostRatioConvert.multiply(bd).setScale(2, BigDecimal.ROUND_HALF_UP);



            // 运营成本
//            BigDecimal operateCost = revenue.multiply(operateCostRatio);
            adMap.put("operateCost", roundHalfUp(operateCost)); // 收入*运营成本占比




//            adMap.put("profit", "$" + profit + " (" + profitRate + "%" +  ")");
/*
            adMap.put("profit", roundHalfUp(profit));

            adMap.put("profitRate", roundHalfUp(profitRate));

            // 物流成本
//            adMap.put("logisticCost", "$" + logisticCost + " (" + logisticCostRatio + "%" +  ")");
            adMap.put("logisticCost", roundHalfUp(logisticCost));
            // 商品成本
//            adMap.put("goodsCost", "$" + goodsCost + " (" + goodsCostRatio + "%" +  ")");
            adMap.put("goodsCost", roundHalfUp(goodsCost));
            // 运营成本
//            adMap.put("operateCost", "$" + operateCost + " (" + operateCostRatio + "%" +  ")");
//            adMap.put("operateCost", "$" + operateCost);
            // 退款
//            adMap.put("refund", "$" + refund + " (" + refundRate + "%" +  ")");
            adMap.put("refund", roundHalfUp(refund));
            // 工具成本
//            adMap.put("toolCost", "$" + toolCost + " (" + toolCostRatio + "%" +  ")");
            adMap.put("toolCost", roundHalfUp(toolCost));
            // 通道成本
//            adMap.put("passCost", "$" + passCost + " (" + passCostRatio + "%" +  ")");
            adMap.put("passCost", roundHalfUp(passCost));
            // 利润
//            adMap.put("profit", "$" + profit + " (" + profitRate + "%" +  ")");
            adMap.put("profit", roundHalfUp(profit));
*/


// 利润
            // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
            BigDecimal profit = revenue.subtract(cost).subtract(logisticCost).subtract(goodsCost).subtract(operateCost).subtract(refund).subtract(toolCost).subtract(passCost);

            // 利润率
            // 利润/收入
            boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;
//            System.out.println("zero:"+zero);
            BigDecimal profitRate = new BigDecimal(0.00);
            if(revenueZero){
//                adMap.put("profitRate", "$" + "0");
            }else{
                profitRate = profit.divide(revenue, 4, BigDecimal.ROUND_HALF_UP);
//                adMap.put("profitRate", "$" + profitRate);
            }

            // 利润
//            adMap.put("profit", "$" + profit + " (" + profitRate + "%" +  ")");
            adMap.put("profit", roundHalfUp(profit));
            adMap.put("profitRate", roundHalfUp(profitRate.multiply(change)));

        }
        return adList;
    }


    /*public List<Map<String, Object>> selectAdChannel(Map map){
        List<Map<String, Object>> adList = adMapper.selectAdChannel(map);

        // 0525


        for(Map<String, Object> adMap : adList){
            BigDecimal revenue = (BigDecimal)adMap.get("revenue");
            BigDecimal cost = (BigDecimal)adMap.get("cost");


            // ROAS
            boolean zero = cost.compareTo(BigDecimal.ZERO)==0;
//            System.out.println("zero:"+zero);

            BigDecimal roas = new BigDecimal(0.00);
            roas.setScale(2, BigDecimal.ROUND_HALF_UP);
            if(zero){
                adMap.put("roas", "0.00");
            }else{
                adMap.put("roas", revenue.divide(cost,2, RoundingMode.HALF_UP));
                roas = revenue.divide(cost, 2, RoundingMode.HALF_UP);
            }

            // 广告成本占比
            boolean zero2 = revenue.compareTo(BigDecimal.ZERO)==0;
            BigDecimal costProportion = new BigDecimal(0.00);
            costProportion.setScale(2, BigDecimal.ROUND_HALF_UP);
//            System.out.println("zero:"+zero);
            if(zero2){
                adMap.put("costProportion", "0");
//                costProportion = "0.00;
            }else{
                adMap.put("costProportion", cost.divide(revenue,2, RoundingMode.HALF_UP));
                costProportion = cost.divide(revenue,2, RoundingMode.HALF_UP);
            }


//            adMap.put("revenue", "$" + revenue + " (" + roas + "%" +  ")");
            adMap.put("revenue", roundHalfUp(revenue));
//            adMap.put("cost", "$" + cost + " (" + costProportion + "%" +  ")");
            adMap.put("cost", roundHalfUp(cost));


*//*
            if(adMap.get("operateCostRatio") == null || adMap.get("logisticCost") == null){
                continue;
            }
*//*


//            BigDecimal operateCost = new BigDecimal(operateCostStr);// 运营成本
            BigDecimal operateCostRatio = new BigDecimal(String.valueOf(adMap.get("operateCostRatio")));// 运营成本占比
            BigDecimal logisticCost = new BigDecimal(String.valueOf(adMap.get("logisticCost"))); // 物流成本
            BigDecimal logisticCostRatio = new BigDecimal(String.valueOf(adMap.get("logisticCostRatio"))); // 物流成本占比
            BigDecimal goodsCost = new BigDecimal(String.valueOf(adMap.get("goodsCost"))); // 商品成本
            BigDecimal goodsCostRatio = new BigDecimal(String.valueOf(adMap.get("goodsCostRatio"))); // 商品成本占比
            BigDecimal refund = new BigDecimal(String.valueOf(adMap.get("refund"))); // 退款
            BigDecimal refundRate = new BigDecimal(String.valueOf(adMap.get("refundRate"))); // 退款率
            BigDecimal toolCost = new BigDecimal(String.valueOf(adMap.get("toolCost"))); // 工具成本
            BigDecimal toolCostRatio = new BigDecimal(String.valueOf(adMap.get("toolCostRatio"))); // 工具成本占比
            BigDecimal passCost = new BigDecimal(String.valueOf(adMap.get("passCost"))); // 通道成本
            BigDecimal passCostRatio = new BigDecimal(String.valueOf(adMap.get("passCostRatio"))); // 通道成本占比

//            Double operateCostRatioDouble = roundHalfUp(operateCostRatio);
            operateCostRatio = roundHalfUp(operateCostRatio);

            adMap.put("operateCostRatio", roundHalfUp(operateCostRatio));
            adMap.put("logisticCostRatio", roundHalfUp(logisticCostRatio));
            adMap.put("goodsCostRatio", roundHalfUp(goodsCostRatio));
            adMap.put("refundRate", roundHalfUp(refundRate));
            adMap.put("toolCostRatio", roundHalfUp(toolCostRatio));
            adMap.put("passCostRatio", roundHalfUp(passCostRatio));

            // 运营成本
            BigDecimal operateCost = revenue.multiply(operateCostRatio);
            adMap.put("operateCost", roundHalfUp(operateCost)); // 收入*运营成本占比


            // 利润
            // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
            BigDecimal profit = revenue.subtract(cost).subtract(logisticCost).subtract(goodsCost).subtract(operateCost).subtract(refund).subtract(toolCost).subtract(passCost);

            // 利润率
            // 利润/收入
            boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;
//            System.out.println("zero:"+zero);
            BigDecimal profitRate = new BigDecimal(0.00);
            if(revenueZero){
//                adMap.put("profitRate", "$" + "0");
            }else{
                profitRate = profit.divide(revenue, 2, BigDecimal.ROUND_HALF_UP);
//                adMap.put("profitRate", "$" + profitRate);
            }

//            adMap.put("profit", "$" + profit + " (" + profitRate + "%" +  ")");
            adMap.put("profit", roundHalfUp(profit));

            adMap.put("profitRate", roundHalfUp(profitRate));

            // 物流成本
//            adMap.put("logisticCost", "$" + logisticCost + " (" + logisticCostRatio + "%" +  ")");
            adMap.put("logisticCost", roundHalfUp(logisticCost));
            // 商品成本
//            adMap.put("goodsCost", "$" + goodsCost + " (" + goodsCostRatio + "%" +  ")");
            adMap.put("goodsCost", roundHalfUp(goodsCost));
            // 运营成本
//            adMap.put("operateCost", "$" + operateCost + " (" + operateCostRatio + "%" +  ")");
//            adMap.put("operateCost", "$" + operateCost);
            // 退款
//            adMap.put("refund", "$" + refund + " (" + refundRate + "%" +  ")");
            adMap.put("refund", roundHalfUp(refund));
            // 工具成本
//            adMap.put("toolCost", "$" + toolCost + " (" + toolCostRatio + "%" +  ")");
            adMap.put("toolCost", roundHalfUp(toolCost));
            // 通道成本
//            adMap.put("passCost", "$" + passCost + " (" + passCostRatio + "%" +  ")");
            adMap.put("passCost", roundHalfUp(passCost));
            // 利润
//            adMap.put("profit", "$" + profit + " (" + profitRate + "%" +  ")");
            adMap.put("profit", roundHalfUp(profit));

        }
        return adList;
    }*/

    public int selectAdChannelCount(Map map){
        int adCount = adMapper.selectAdChannelCount(map);
        return adCount;
    }

    /*public int selectChannelDataCount(Map map){
        int adCount = adMapper.selectChannelDataCount(map);
        return adCount;
    }*/

    public void deleteByType(Map map){
        adMapper.deleteByType(map);
    }

    public void deleteByItemsId(Map map){
        adMapper.deleteByItemsId(map);
    }

    public void deleteByAdAccount(Map map){
        adMapper.deleteByAdAccount(map);
    }

    public void syncGoogleData(){



        try {
            Map maps = new HashMap();
            List<Map<String, Object>> certificateList = certificateService.selectAllCertificate(maps);
            System.out.println("certificateList："+certificateList);

            if(certificateList.size() > 0){
                Map<String, Object> certificateMap = certificateList.get(0);
                String serviceAccountId = (String)certificateMap.get("service_account_id");
                String path = (String)certificateMap.get("path");


                File orgFile = new File(File.listRoots()[UploadConstant.UPLOAD_DIS_INDEX], UploadConstant.UPLOAD_DIR + path);

                List<AccountSummary> itemList = AnalyticsUtil.getItems(serviceAccountId, orgFile.getPath());

                System.out.println("itemList:"+itemList);

                String yesterdayDate = DateUtil.timestampToTime(System.currentTimeMillis() - 86400000, "yyyy-MM-dd");
                System.out.println("yesterdayDate："+yesterdayDate);

                // 先删除数据再重新记录
                Map adMap = new HashMap();
                adMap.put("type", 0);
                adMap.put("create_time", DateUtil.timestampToTime(System.currentTimeMillis()-86400000, "yyyy-MM-dd"));
                adMapper.deleteByType(adMap);

                //            itemsService.deleteAllItems();
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
                                System.out.println("adData："+adData);

                                logger.error("adTasks adData：" + adData);

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

                                        // 截取广告名称中的工号
                                        String result2 = adName.substring(0, adName.indexOf("["));
                                        String jobNumber = adName.substring(result2.length()+1, adName.length()-1);

                                        map.put("job_number", jobNumber);
                                        map.put("ad_account", adAccount);
                                        map.put("ad_name", adName);
                                        map.put("source", source);
                                        map.put("revenue", revenue);
                                        map.put("cost", cost);
                                        map.put("type", 0);
                                        map.put("create_time", DateUtil.timestampToTime(System.currentTimeMillis()-86400000, "yyyy-MM-dd"));

                                        System.out.println("map:"+map);

                                        adMapper.insertAd(map);
                                    }
                                }

                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    // 同步FB广告数据
    public void syncFacebookData(){

        try {

            logger.error("开始同步FB广告啦："+DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss:SSS"));

            // 获取广告账户
            MultiValueMap<String, String> adAccountParams = new LinkedMultiValueMap<>();
            adAccountParams.add("access_token", ACCESS_TOKEN);
            adAccountParams.add("fields", "id,name,account_id,spend");

            String adAccountUrl = GRAPH_URL + BUSINESS_ID + "/client_ad_accounts?";
            logger.error("adAccountParams："+adAccountParams);


            String fields = "id,name,account_id,spend";

            adAccountUrl = adAccountUrl + "access_token=" + ACCESS_TOKEN + "&fields=" + fields;

            logger.error("adAccountUrl："+adAccountUrl);


            String adAccountResult = HttpUtil.get(adAccountUrl, adAccountParams);
//        String adAccountResult = "{\"data\":[{\"id\":\"act_2523536311212491\",\"name\":\"FAYN-MS-dafunia02\",\"account_id\":\"2523536311212491\"},{\"id\":\"act_578026722972031\",\"name\":\"FAYN-MS-dafunia01\",\"account_id\":\"578026722972031\"},{\"id\":\"act_837155403286860\",\"name\":\"FADM-MS-sonsoulier04\",\"account_id\":\"837155403286860\"},{\"id\":\"act_381293539350908\",\"name\":\"FADM-MS-sonsoulier03\",\"account_id\":\"381293539350908\"},{\"id\":\"act_2292495061017464\",\"name\":\"FADM-MS-sonsoulier02\",\"account_id\":\"2292495061017464\"},{\"id\":\"act_532421010599795\",\"name\":\"FADM-MS-sonsoulier01\",\"account_id\":\"532421010599795\"},{\"id\":\"act_2189317301321438\",\"name\":\"FADM-MS-Shoeri03\",\"account_id\":\"2189317301321438\"},{\"id\":\"act_292436271463301\",\"name\":\"FADM-MS-Shoeri04\",\"account_id\":\"292436271463301\"},{\"id\":\"act_231919237755305\",\"name\":\"FADM-MS-Shoeri05\",\"account_id\":\"231919237755305\"},{\"id\":\"act_2160394957354192\",\"name\":\"FADM-MS-Shoeri01\",\"account_id\":\"2160394957354192\"},{\"id\":\"act_1011028935762570\",\"name\":\"FADM-MS-Shoeri02\",\"account_id\":\"1011028935762570\"}],\"paging\":{\"cursors\":{\"before\":\"QVFIUlREOHNaYXpkSV9MRWRjYlREaUR1WnI0ek55VEhwT25CZAlNtczlmeXA1MWJ6MHVtbjBJeEtpN1psZAXVmMmthaVhVUV8zRENSRXB1aDdFUzkxMkZASM0tn\",\"after\":\"QVFIUnNkZA0xnbVBRT2Y2aUJqSjZAlOHVNTDc3Nk4tNEVVSzVzMFNLRFhtaTFFQnJ0b3QtNkV2ejl3cWcxejZARMm5USHlvWVByel9CUWlPQ2VRREwtQjI4V053\"}}}";
            System.out.println("adAccountResult:"+adAccountResult);


            logger.error("adAccountResult："+adAccountResult);


            JSONObject adAccountObject = JSONObject.parseObject(adAccountResult);
            JSONArray adAccountDataArr = adAccountObject.getJSONArray("data");



            // 先删除数据再重新记录
            Map adMap = new HashMap();
            adMap.put("type", 1);
            adMap.put("create_time", DateUtil.timestampToTime(System.currentTimeMillis()-86400000, "yyyy-MM-dd"));
            adMapper.deleteByType(adMap);

            for(int i = 0; i < adAccountDataArr.size(); i++){

                JSONObject adAccountObj = adAccountDataArr.getJSONObject(i);
                String adAccountId = adAccountObj.getString("account_id");


                // 获取广告系列
                String campaignsUrl = GRAPH_URL + "act_" + adAccountId + "/campaigns?";
                MultiValueMap<String, String> campaignsParams = new LinkedMultiValueMap<>();
                campaignsParams.add("access_token", ACCESS_TOKEN);
                campaignsParams.add("limit", "100");
                campaignsParams.add("fields", "name,start_time,objective,status,spend");


                String campaignsFields = "name,start_time,objective,status,spend";
                campaignsUrl = campaignsUrl + "access_token=" + ACCESS_TOKEN + "&limit=" + 100 + "&fields=" + campaignsFields;
                logger.error("campaignsUrl:"+campaignsUrl);


                String campaignsResult = HttpUtil.get(campaignsUrl, campaignsParams);
                //            String campaignsResult = "{\"data\":[{\"name\":\"SYV1578[110]\",\"start_time\":\"2020-06-10T11:41:33+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"ACTIVE\",\"id\":\"23844857760240787\"},{\"name\":\"SYV1582-df1-YJW\",\"start_time\":\"2020-06-09T11:34:42+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844855998160787\"},{\"name\":\"SYV1571-df1-2-YJW\",\"start_time\":\"2020-06-08T09:31:18+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844850181450787\"},{\"name\":\"SYV1323-testvideo-df1-YJW\",\"start_time\":\"2020-06-06T11:48:19+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844839107080787\"},{\"name\":\"SYV1249-testvideo-df1-YJW\",\"start_time\":\"2020-06-06T11:25:53+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844839037590787\"},{\"name\":\"SYV1571-df1-YJW\",\"start_time\":\"2020-06-05T12:00:30+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844837835820787\"},{\"name\":\"SYV1556[110]\",\"start_time\":\"2020-06-04T11:54:40+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844825691130787\"},{\"name\":\"SYV1526-2-YJW\",\"start_time\":\"2020-06-02T13:51:34+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844819878090787\"},{\"name\":\"SYV1527-2-YJW\",\"start_time\":\"2020-06-02T10:13:27+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844819241890787\"},{\"name\":\"SYV1528-YJW\",\"start_time\":\"2020-05-30T12:00:23+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844807448600787\"},{\"name\":\"SYV1527-YJW\",\"start_time\":\"2020-05-30T11:50:48+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844807428550787\"},{\"name\":\"SYV1524-YJW\",\"start_time\":\"2020-05-30T11:35:03+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844807381730787\"},{\"name\":\"SYV1526-YJW\",\"start_time\":\"2020-05-30T11:27:30+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"ACTIVE\",\"id\":\"23844807361560787\"},{\"name\":\"SYV1225-YJW\",\"start_time\":\"2020-05-30T11:18:02+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844804540840787\"},{\"name\":\"SYV1515-YJW\",\"start_time\":\"2020-05-29T14:26:31+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844802797910787\"},{\"name\":\"SYV1439-YJW\",\"start_time\":\"2020-05-28T10:59:03+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844796492150787\"},{\"name\":\"SYV1487-audience-YJW\",\"start_time\":\"2020-05-26T10:58:14+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844785100060787\"},{\"name\":\"SYV1119-audience-YJW\",\"start_time\":\"2020-05-26T10:29:28+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844784996030787\"},{\"name\":\"SYV1487-YJW\",\"start_time\":\"2020-05-25T10:20:22+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844779987430787\"},{\"name\":\"SYV1436-DF01-3-YJW\",\"start_time\":\"2020-05-23T19:02:38+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844773018940787\"},{\"name\":\"SYV1414-4-YJW\",\"start_time\":\"2020-05-23T19:01:34+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844773013420787\"},{\"name\":\"hity♥0415-2-YJW\",\"start_time\":\"2020-05-19T16:33:44+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844750412530787\"},{\"name\":\"SYV1436-DF01-2-YJW\",\"start_time\":\"2020-05-17T00:51:09+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844739118090787\"},{\"name\":\"SYV1414-3-YJW\",\"start_time\":\"2020-05-16T11:17:38+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844736766270787\"},{\"name\":\"SYV1436-DF01-YJW\",\"start_time\":\"2020-05-16T10:50:15+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844736638860787\"},{\"name\":\"SYV1119-2-YJW\",\"start_time\":\"2020-05-15T17:33:01+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844732440440787\"},{\"name\":\"SYV1414-2-YJW\",\"start_time\":\"2020-05-14T15:48:37+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844725210360787\"},{\"name\":\"SYV1291-0514-YJW\",\"start_time\":\"2020-05-14T10:32:27+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844722827290787\"},{\"name\":\"SYV1414-YJW\",\"start_time\":\"2020-05-14T10:07:32+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844722546820787\"},{\"name\":\"SYV1394-YJW\",\"start_time\":\"2020-05-08T10:37:41+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844686238770787\"},{\"name\":\"SYV1355-YJW\",\"start_time\":\"2020-05-08T10:20:23+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844686223550787\"},{\"name\":\"SYV1325-YJW\",\"start_time\":\"2020-04-26T14:07:59+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844626091150787\"},{\"name\":\"SYV1339-YJW\",\"start_time\":\"2020-04-24T11:05:33+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844612827330787\"},{\"name\":\"YBL0355-YJW\",\"start_time\":\"2020-04-23T11:04:47+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844610970260787\"},{\"name\":\"ubrania-bs-YJW\",\"start_time\":\"2020-04-23T11:49:03+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844610968690787\"},{\"name\":\"SYV1126-新图-YJW\",\"start_time\":\"2020-04-23T11:26:40+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844606548070787\"},{\"name\":\"YBL0332-YJW\",\"start_time\":\"2020-04-22T11:10:21+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844605725000787\"},{\"name\":\"YBL0327-YJW\",\"start_time\":\"2020-04-22T10:54:47+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844605673690787\"},{\"name\":\"YBL0356-YJW\",\"start_time\":\"2020-04-22T10:36:45+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844605603690787\"},{\"name\":\"hity!0421-YJW\",\"start_time\":\"2020-04-21T12:26:35+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844601179180787\"},{\"name\":\"SYV1314-YJW\",\"start_time\":\"2020-04-21T10:28:49+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844598171410787\"},{\"name\":\"SYV1291-2-YJW\",\"start_time\":\"2020-04-20T16:32:56+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844596513750787\"},{\"name\":\"Hot花0420-YJW\",\"start_time\":\"2020-04-20T11:58:18+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844595305680787\"},{\"name\":\"POMOCJA花0420-YJW\",\"start_time\":\"2020-04-20T10:30:57+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844594945680787\"},{\"name\":\"SYV1001-自摄视频-YJW\",\"start_time\":\"2020-04-18T10:44:39+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844579440000787\"},{\"name\":\"SYV1147-2-PL\",\"start_time\":\"2020-04-17T14:26:41+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844579155310787\"},{\"name\":\"SYV1291-YJW\",\"start_time\":\"2020-04-17T14:18:55+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844579134570787\"},{\"name\":\"Hot sale0416-YJW\",\"start_time\":\"2020-04-16T11:18:39+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844571938950787\"},{\"name\":\"SYV1244-2-YJW\",\"start_time\":\"2020-04-16T13:46:43+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844571845500787\"},{\"name\":\"SYV1119-YJW\",\"start_time\":\"2020-04-16T11:10:06+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"ACTIVE\",\"id\":\"23844567962020787\"},{\"name\":\"Kwietnia0415-YJW\",\"start_time\":\"2020-04-15T11:06:47+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844564475070787\"},{\"name\":\"hity♥0415-YJW\",\"start_time\":\"2020-04-15T10:00:07+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844560975160787\"},{\"name\":\"SYV1249-新图新链-YJW\",\"start_time\":\"2020-04-15T09:57:01+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844560890160787\"},{\"name\":\"SYV1147-2-PL\",\"start_time\":\"2020-04-13T10:13:48+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844553516910787\"},{\"name\":\"SYV1147-PL\",\"start_time\":\"2020-04-12T10:19:19+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844546066010787\"},{\"name\":\"SYV1249-video-YJW\",\"start_time\":\"2020-04-10T10:50:48+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844535532070787\"},{\"name\":\"SYZ0674-YJW\",\"start_time\":\"2020-04-10T09:48:22+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844531616940787\"},{\"name\":\"SYV1255-YJW\",\"start_time\":\"2020-04-10T10:09:36+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844531486200787\"},{\"name\":\"SYV0040-0409-YJW\",\"start_time\":\"2020-04-09T14:31:31+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844530949380787\"},{\"name\":\"SYV1253-YJW\",\"start_time\":\"2020-04-09T09:57:48+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844526418920787\"},{\"name\":\"SYV1249-YJW\",\"start_time\":\"2020-04-09T09:47:08+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844526387170787\"},{\"name\":\"SYV1246-YJW\",\"start_time\":\"2020-04-09T10:00:01+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844526309410787\"},{\"name\":\"SYV1244-YJW\",\"start_time\":\"2020-04-09T10:00:37+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844526274330787\"},{\"name\":\"SYV1101-YJW\",\"start_time\":\"2020-04-08T10:15:11+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844521379820787\"},{\"name\":\"SYV0890-YJW\",\"start_time\":\"2020-04-08T10:06:12+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844521344270787\"},{\"name\":\"ubrania-nowosc-0407-YJW\",\"start_time\":\"2020-04-08T12:16:13+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844521259890787\"},{\"name\":\"SYV1180-YJW\",\"start_time\":\"2020-04-03T09:31:16+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844502772150787\"},{\"name\":\"SYV1196-2-YJW\",\"start_time\":\"2020-04-02T11:06:17+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844498259010787\"},{\"name\":\"SYV1047-YJW\",\"start_time\":\"2020-04-02T10:17:20+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844494927940787\"},{\"name\":\"SYV0983-YJW\",\"start_time\":\"2020-04-01T09:53:16+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844493014990787\"},{\"name\":\"SYV0322-YJW\",\"start_time\":\"2020-04-01T10:30:10+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844491336090787\"},{\"name\":\"SYV0944-YJW\",\"start_time\":\"2020-04-01T10:07:51+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844491238050787\"},{\"name\":\"SYV1167-YJW\",\"start_time\":\"2020-03-31T10:47:03+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844488219500787\"},{\"name\":\"SYV1166-YJW\",\"start_time\":\"2020-03-31T10:29:02+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844488152400787\"},{\"name\":\"SYV1190-YJW\",\"start_time\":\"2020-03-31T10:00:32+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844486029680787\"},{\"name\":\"SYV1175-YJW\",\"start_time\":\"2020-03-31T10:00:55+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844484787160787\"},{\"name\":\"SYV1179-2-YJW\",\"start_time\":\"2020-03-30T11:23:48+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844483900590787\"},{\"name\":\"SYV1196-YJW\",\"start_time\":\"2020-03-28T10:00:12+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844473513130787\"},{\"name\":\"OBL03368-YJW\",\"start_time\":\"2020-03-26T11:15:02+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844468583740787\"},{\"name\":\"SYV0976-YJW\",\"start_time\":\"2020-03-26T10:00:50+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844465619580787\"},{\"name\":\"SYV1179-YJW\",\"start_time\":\"2020-03-26T10:00:41+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844465272550787\"},{\"name\":\"SYV1183-YJW\",\"start_time\":\"2020-03-25T10:00:46+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844461741020787\"},{\"name\":\"SYV1187-YJW\",\"start_time\":\"2020-03-25T10:10:39+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844461704760787\"},{\"name\":\"OB0048-YJW\",\"start_time\":\"2020-03-25T10:00:55+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844461600360787\"},{\"name\":\"OB0230-YJW\",\"start_time\":\"2020-03-25T10:00:17+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844461483460787\"},{\"name\":\"OB0269-YJW\",\"start_time\":\"2020-03-24T11:24:46+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844460781000787\"},{\"name\":\"OB0268-YJW\",\"start_time\":\"2020-03-24T11:09:56+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844460720990787\"},{\"name\":\"OBL03358-YJW\",\"start_time\":\"2020-03-23T09:59:37+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844455051760787\"},{\"name\":\"OBL03356-YJW\",\"start_time\":\"2020-03-23T10:59:20+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844455015920787\"},{\"name\":\"OBL03353\",\"start_time\":\"2020-03-22T10:00:00+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844449992660787\"},{\"name\":\"SYV1162-YJW\",\"start_time\":\"2020-03-21T11:09:06+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844449294860787\"},{\"name\":\"SYV0040-YJW\",\"start_time\":\"2020-03-20T10:44:36+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844445118930787\"},{\"name\":\"SYV1153-YJW\",\"start_time\":\"2020-03-20T09:54:11+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844444956060787\"},{\"name\":\"SYV0701-2-YJW\",\"start_time\":\"2020-03-20T09:31:43+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844444906780787\"},{\"name\":\"SYV1116-YJW\",\"start_time\":\"2020-03-20T10:00:07+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844441299210787\"},{\"name\":\"SYV1155-YJW\",\"start_time\":\"2020-03-19T09:34:33+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844440901410787\"},{\"name\":\"SYV1156-YJW\",\"start_time\":\"2020-03-19T10:04:49+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844439277230787\"},{\"name\":\"SYV1154--YJW\",\"start_time\":\"2020-03-19T10:00:45+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844439182980787\"},{\"name\":\"Na wiosnę0317-YJW\",\"start_time\":\"2020-03-18T10:00:38+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844433901770787\"},{\"name\":\"SYV0701-YJW\",\"start_time\":\"2020-03-18T10:00:38+0800\",\"objective\":\"CONVERSIONS\",\"status\":\"PAUSED\",\"id\":\"23844433459410787\"}],\"paging\":{\"cursors\":{\"before\":\"QVFIUjhOMDdjTTVTR3JkQXgyNUM2OVNKUjgzLWJmMEt1ZAzA5NkJwWFlqZAW1BY1h3cHhMdTVXR0VIYXFjY0FMVlNjbmhCT0loYjBfQ2JMQTcyb1BmbVI1eHVB\",\"after\":\"QVFIUlkybW9iZAF9yZAzFkcE0zM2drYnBHbWFZAZA0F1RWhHaDlYdUtLcDhCc3ZA2bU4wbmVyTldsWU85ZA1BQUUpNSm5xRUpPTVNpSFBrN2wwczBqM1VaN0dra21R\"},\"next\":\"https://graph.facebook.com/v7.0/act_578026722972031/campaigns?access_token=EAAH92JtasVMBAJ2iHbMXEdLwzMZAH2PidkMGwvQbhFZCZAAcPmUHOxfwaPfNg4M3vXCBonOVZAHLIrj7gdZCJqT9pQs8CAMGrBp7ECuNKOdFIO5txnP3UylNAI959oXBqp1hZAJloEBqSvVdt3hVhXYDu7WGdoZCgZCqrqX0PVE5LKKdGtlzQMxZBmrY8YWjQARUZD&fields=name%2Cstart_time%2Cobjective%2Cstatus%2Ccost_per_conversion%2Cspend&sort=%5B%22name_descending%22%5D&date_preset=this_week_sun_today&limit=100&after=QVFIUlkybW9iZAF9yZAzFkcE0zM2drYnBHbWFZAZA0F1RWhHaDlYdUtLcDhCc3ZA2bU4wbmVyTldsWU85ZA1BQUUpNSm5xRUpPTVNpSFBrN2wwczBqM1VaN0dra21R\"}}";
                System.out.println("campaignsResult:"+campaignsResult);
                logger.error("campaignsResult:"+campaignsResult);


                JSONObject campaignsObjs = JSONObject.parseObject(campaignsResult);
                JSONArray campaignsDataArr = campaignsObjs.getJSONArray("data");
                for(int a = 0; a < campaignsDataArr.size(); a++){
                    JSONObject campaignsObj = campaignsDataArr.getJSONObject(a);
                    String campaignsId = campaignsObj.getString("id");
                    String campaignsName = campaignsObj.getString("name");

                    boolean is = campaignsName.contains("[") && campaignsName.contains("]");
                    if(!is){
                        continue;
                    }

                    // 获取广告系列详情
                    String insightsUrl = GRAPH_URL + campaignsId + "/insights?";
                    MultiValueMap<String, String> insightsParams = new LinkedMultiValueMap<>();
                    insightsParams.add("access_token", ACCESS_TOKEN);
//                    insightsParams.add("fields", "id,name,account_id,spend");
                    String data = DateUtil.timestampToTime(System.currentTimeMillis() - 86400000, "yyyy-MM-dd");
//                    insightsParams.add("time_range", "{'since':'"+data+"','until':'"+data+"'}"); // 查询昨天的

//                    String insightsFields = "id,name,account_id,spend";
                    String time_range = "{'since':'"+data+"','until':'"+data+"'}";
//                    insightsUrl = insightsUrl + "access_token=" + ACCESS_TOKEN + "&time_range=" + time_range;

                    logger.error("insightsUrl2:"+insightsUrl);

                    Map<String,String> params = new HashMap<>();
                    params.put("access_token", ACCESS_TOKEN);
                    params.put("time_range", time_range);
                    params.put("fields", "account_id,campaign_id,impressions,spend,account_name,campaign_name,purchase_roas");

//                    String insightsResult = HttpUtil.get(insightsUrl, insightsParams);
                    String insightsResult = HttpUtil.getInstance().doGet(insightsUrl, params);


                    //                String insightsResult = "{\"data\":[{\"account_id\":\"578026722972031\",\"campaign_id\":\"23844857760240787\",\"impressions\":\"14009\",\"spend\":\"53.06\",\"account_name\":\"FAYN-MS-dafunia01\",\"campaign_name\":\"SYV1578[110]\",\"purchase_roas\":[{\"action_type\":\"omni_purchase\",\"value\":\"3.403694\"}],\"date_start\":\"2020-06-12\",\"date_stop\":\"2020-06-12\"}],\"paging\":{\"cursors\":{\"before\":\"MAZDZD\",\"after\":\"MAZDZD\"}}}";
                    System.out.println("insightsResult:"+insightsResult);

                    logger.error("insightsResult:"+insightsResult);


                    JSONObject insightsObjs = JSONObject.parseObject(insightsResult);
                    JSONArray insightsDataArr = insightsObjs.getJSONArray("data");
                    for(int b = 0; b < insightsDataArr.size(); b++){
                        JSONObject insightsObj = insightsDataArr.getJSONObject(0);
                        Double spend = insightsObj.getDouble("spend"); // 成本
                        String date = insightsObj.getString("date_start");


                        JSONArray purchaseRoasArr = insightsObj.getJSONArray("purchase_roas"); // 花费回报
                        Double value = 0.00;
                        if(purchaseRoasArr != null){
                            JSONObject purchaseRoasObj = purchaseRoasArr.getJSONObject(0);
                            value = purchaseRoasObj.getDouble("value");
                        }


                        Map map = new HashMap<>();
                        map.put("items_id", adAccountId);
                        //                    map.put("profiles_id", profileId);

                        logger.error("campaignsName:"+campaignsName);
                        // 截取广告名称中的工号
                        String result2 = campaignsName.substring(0, campaignsName.indexOf("["));
                        String jobNumber = campaignsName.substring(result2.length()+1, campaignsName.length()-1);

                        map.put("job_number", jobNumber);
                        map.put("ad_account", adAccountId);
                        map.put("ad_name", campaignsName);
                        map.put("source", "facebook.com/cpc"); // 固定不变 写死

                        BigDecimal revenue = new BigDecimal(spend*value).setScale(2, RoundingMode.HALF_UP);
                        map.put("revenue", String.format("%.2f", revenue)); // 收入
                        map.put("cost", spend); // 成本
                        //                    map.put("create_time", DateUtil.timestampToTime(System.currentTimeMillis()-86400000, "yyyy-MM-dd"));
                        map.put("type", 1);
                        map.put("create_time", date);

                        System.out.println("map:"+map);

                        adMapper.insertAd(map);

                    }

                }



            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("itemsTasks2 e:"+e);
        }
    }


    public Float selectAMonthRevenue(Map map){
        Float revenue = adMapper.selectAMonthRevenue(map);
        return revenue;
    }

    public BigDecimal roundHalfUp(BigDecimal value){
        return value.setScale(2, BigDecimal.ROUND_HALF_UP);
    }


}
