package com.egao.common.core.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * JSON解析工具类
 * Created by wangfan on 2017-06-10 10:10
 */
public class CostUtil {

    public static void censusItemCost(Map map, List<Map<String, Object>> tempList, List<Map<String, Object>> itemList, List<Map<String, Object>> costList
            , List<Map<String, Object>> overallList, List<Map<String, Object>> itemCostList
            , String startTime, String endTime){

        int distanceOfTwoDate = DateUtil.getDistanceOfTwoDate(startTime, endTime); // 获取两个日期的天数
        System.out.println("distanceOfTwoDate："+distanceOfTwoDate);

        String startMonth = DateUtil.timestampToTime(DateUtil.parseDateStr(startTime, "yyyy-MM-dd").getTime());
        String endMonth = DateUtil.timestampToTime(DateUtil.parseDateStr(endTime, "yyyy-MM-dd").getTime());

        System.out.println("censusItemCost itemList："+JSONArray.toJSONString(itemList));
        System.out.println("censusItemCost costList："+JSONArray.toJSONString(costList));
        System.out.println("censusItemCost itemCostList："+JSONArray.toJSONString(itemCostList));


        for(Map<String, Object> itemMap : itemList){
            String itemMonth = (String)itemMap.get("month");
            Long itemsId = (Long)itemMap.get("itemsId");
            BigDecimal revenue = (BigDecimal)itemMap.get("revenue");
            BigDecimal cost = (BigDecimal)itemMap.get("cost");
            BigDecimal channelCost = (BigDecimal)itemMap.get("channelCost");


            boolean isCost = false;
            boolean isAdjoinCost = false;
            // 站点成本
            for(Map<String, Object> costMap : costList){
                String costMonth = (String)costMap.get("month");
                Long costItemId = (Long)costMap.get("item_id");


                if(itemsId.equals(costItemId) && itemMonth.equals(costMonth)){
                    System.out.println("站点成本");
                    BigDecimal logisticCost = (BigDecimal)costMap.get("logisticCost");
                    BigDecimal goodsCost = (BigDecimal)costMap.get("goodsCost");
                    BigDecimal refund = (BigDecimal)costMap.get("refund");
                    BigDecimal toolCost = (BigDecimal)costMap.get("toolCost");
                    BigDecimal passCost = (BigDecimal)costMap.get("passCost");
                    BigDecimal operateCost = (BigDecimal)costMap.get("operateCost");


                    int monthLastDay = DateUtil.getMonthLastDay(DateUtil.parseDateStr(itemMonth, "yyyy-MM"));
                    BigDecimal currentMonthDay = new BigDecimal(monthLastDay);

                    BigDecimal distanceDay = getDistanceDay(startTime, endTime, startMonth, endMonth, itemMonth);

                    BigDecimal change = new BigDecimal(100);


                    // 获取当月收入
                    BigDecimal currentMonthRevenue = new BigDecimal(0.00);
                    for(Map<String, Object> itemCostMap : itemCostList){
                        Long itemCostItemId = (Long)itemCostMap.get("itemsId");
                        String itemCostMonth = (String)itemCostMap.get("month");
                        if(itemsId.equals(itemCostItemId) && costMonth.equals(itemCostMonth)){
                            currentMonthRevenue = (BigDecimal)itemCostMap.get("revenue");
                            System.out.println("获取当月收入 itemCostMap："+itemCostMap);
                            break;
                        }
                    }

                    boolean currentMonthRevenueZero = currentMonthRevenue.compareTo(BigDecimal.ZERO)==0;

//                    logisticCostRatio = logisticCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP);

                    BigDecimal logisticCostRatio = currentMonthRevenueZero ? currentMonthRevenue : logisticCost.divide(currentMonthRevenue, 6, BigDecimal.ROUND_HALF_UP);

                    logisticCost = revenue.multiply(logisticCostRatio).setScale(6, BigDecimal.ROUND_HALF_UP);

//                    logisticCost = logisticCost.divide(currentMonthDay, 8, BigDecimal.ROUND_HALF_UP).multiply(distanceDay).setScale(2, BigDecimal.ROUND_HALF_UP);
                    itemMap.put("logisticCost", logisticCost.setScale(2, BigDecimal.ROUND_HALF_UP));

//                    goodsCostRatio = goodsCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP);

                    BigDecimal goodsCostRatio = currentMonthRevenueZero ? currentMonthRevenue : goodsCost.divide(currentMonthRevenue, 6, BigDecimal.ROUND_HALF_UP);
                    goodsCost = revenue.multiply(goodsCostRatio).setScale(6, BigDecimal.ROUND_HALF_UP);
//                    goodsCost = goodsCost.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
//                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                    itemMap.put("goodsCost", goodsCost.setScale(2, BigDecimal.ROUND_HALF_UP));

//                    refundRate = refundRate.divide(change, 4, BigDecimal.ROUND_HALF_UP);
                    BigDecimal refundRate = currentMonthRevenueZero ? currentMonthRevenue : refund.divide(currentMonthRevenue, 6, BigDecimal.ROUND_HALF_UP);
                    refund = revenue.multiply(refundRate).setScale(6, BigDecimal.ROUND_HALF_UP);
//                    refund = refund.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
//                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                    itemMap.put("refund", refund.setScale(2, BigDecimal.ROUND_HALF_UP));

//                    toolCostRatio = toolCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP);
                    BigDecimal toolCostRatio = currentMonthRevenueZero ? currentMonthRevenue : toolCost.divide(currentMonthRevenue, 6, BigDecimal.ROUND_HALF_UP);
                    toolCost = revenue.multiply(toolCostRatio).setScale(6, BigDecimal.ROUND_HALF_UP);
//                    toolCost = toolCost.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
//                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                    itemMap.put("toolCost", toolCost.setScale(2, BigDecimal.ROUND_HALF_UP));

//                    passCostRatio = passCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP);
                    BigDecimal passCostRatio = currentMonthRevenueZero ? currentMonthRevenue : passCost.divide(currentMonthRevenue, 6, BigDecimal.ROUND_HALF_UP);
                    passCost = revenue.multiply(passCostRatio).setScale(6, BigDecimal.ROUND_HALF_UP);
//                    passCost = passCost.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
//                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                    itemMap.put("passCost", passCost.setScale(2, BigDecimal.ROUND_HALF_UP));

//                    operateCostRatio = operateCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP);
                    BigDecimal operateCostRatio = currentMonthRevenueZero ? currentMonthRevenue : operateCost.divide(currentMonthRevenue, 6, BigDecimal.ROUND_HALF_UP);
                    operateCost = revenue.multiply(operateCostRatio).setScale(6, BigDecimal.ROUND_HALF_UP);
//                    operateCost = operateCost.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
//                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                    itemMap.put("operateCost", operateCost.setScale(2, BigDecimal.ROUND_HALF_UP));

                    BigDecimal zero = new BigDecimal(0.00);
                    boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;

                    itemMap.put("logisticCostRatio", revenueZero ? zero : logisticCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("goodsCostRatio", revenueZero ? zero : goodsCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("refundRate", revenueZero ? zero : refund.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("toolCostRatio", revenueZero ? zero : toolCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("passCostRatio", revenueZero ? zero : passCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("operateCostRatio", revenueZero ? zero : operateCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));

                    channelCost = String.valueOf(channelCost).equals("null") ? new BigDecimal(0.00) : channelCost;
                    cost = cost.add(channelCost);
                    itemMap.put("cost", cost.setScale(2, BigDecimal.ROUND_HALF_UP));

                    // ROAS
                    boolean costZero = cost.compareTo(BigDecimal.ZERO)==0;
                    itemMap.put("roas", costZero ? zero : revenue.divide(cost,2, RoundingMode.HALF_UP));

                    // 广告成本占比
                    itemMap.put("costProportion", revenueZero ? zero: (cost.divide(revenue,6, RoundingMode.HALF_UP)).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));


                    // 利润
                    // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
                    BigDecimal profit = revenue.subtract(cost).subtract(logisticCost).subtract(goodsCost).subtract(operateCost).subtract(refund).subtract(toolCost).subtract(passCost);

                    // 利润率
                    // 利润/收入

                    // 利润
                    BigDecimal profitRou = roundHalfUp(profit.setScale(2, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("profit", profitRou);

                    // 判断被除数是否为零
                    BigDecimal profitRate = revenueZero ? zero : profitRou.divide(revenue, 6, BigDecimal.ROUND_HALF_UP);
                    itemMap.put("profitRate", roundHalfUp(profitRate.multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP)));


                    isCost = true;
                    break;
                }

                if(itemsId.equals(costItemId)){
                    isAdjoinCost = true;
                    System.out.println("相邻成本 go");

                }
            }
            // 取最邻近目前日期的月份，或者说最大月份
            if(isAdjoinCost){
                System.out.println("相邻成本 judge");
                for(Map<String, Object> costMap : costList){
                    String costMonth = (String)costMap.get("month");
                    Long costItemId = (Long)costMap.get("item_id");

                    if(itemsId.equals(costItemId)){

                        System.out.println("相邻成本 ok");
                        BigDecimal logisticCost = (BigDecimal)costMap.get("logisticCost");
                        BigDecimal goodsCost = (BigDecimal)costMap.get("goodsCost");
                        BigDecimal refund = (BigDecimal)costMap.get("refund");
                        BigDecimal toolCost = (BigDecimal)costMap.get("toolCost");
                        BigDecimal passCost = (BigDecimal)costMap.get("passCost");
                        BigDecimal operateCost = (BigDecimal)costMap.get("operateCost");

                        int monthLastDay = DateUtil.getMonthLastDay(DateUtil.parseDateStr(itemMonth, "yyyy-MM"));
                        BigDecimal currentMonthDay = new BigDecimal(monthLastDay);

                        BigDecimal distanceDay = getDistanceDay(startTime, endTime, startMonth, endMonth, itemMonth);

                        System.out.println("distanceDay："+distanceDay);

                        BigDecimal change = new BigDecimal(100);

                        // 获取当月收入
                        BigDecimal currentMonthRevenue = new BigDecimal(0.00);
                        for(Map<String, Object> itemCostMap : itemCostList){
                            Long itemCostItemId = (Long)itemCostMap.get("itemsId");
                            String itemCostMonth = (String)itemCostMap.get("month");
//                            if(itemsId.equals(itemCostItemId) && itemMonth.equals(itemCostMonth)){
                            if(itemsId.equals(itemCostItemId) && costMonth.equals(itemCostMonth)){
                                currentMonthRevenue = (BigDecimal)itemCostMap.get("revenue");
                                System.out.println("获取当月收入 itemCostMap："+itemCostMap);
                            }
                        }

                        boolean currentMonthRevenueZero = currentMonthRevenue.compareTo(BigDecimal.ZERO)==0;
                        // 如果相邻成本为0，直接取全局成本
                        if(currentMonthRevenueZero){
                            System.out.println("如果相邻成本为0，直接取全局成本");
                            isCost = false;
                            break;
                        }

//                    logisticCostRatio = logisticCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP);

                        BigDecimal logisticCostRatio = currentMonthRevenueZero ? currentMonthRevenue : logisticCost.divide(currentMonthRevenue, 6, BigDecimal.ROUND_HALF_UP);

                        System.out.println("logisticCostRatio："+logisticCostRatio);
                        System.out.println("logisticCost0："+logisticCost);
                        logisticCost = revenue.multiply(logisticCostRatio).setScale(6, BigDecimal.ROUND_HALF_UP);

                        System.out.println("currentMonthDay："+currentMonthDay);
                        System.out.println("distanceDay："+distanceDay);

                        System.out.println("logisticCost1："+logisticCost);
//                    logisticCost = logisticCost.divide(currentMonthDay, 8, BigDecimal.ROUND_HALF_UP).multiply(distanceDay).setScale(2, BigDecimal.ROUND_HALF_UP);
                        System.out.println("logisticCost2："+logisticCost);
                        itemMap.put("logisticCost", logisticCost.setScale(2, BigDecimal.ROUND_HALF_UP));

//                    goodsCostRatio = goodsCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP);

                        BigDecimal goodsCostRatio = currentMonthRevenueZero ? currentMonthRevenue : goodsCost.divide(currentMonthRevenue, 6, BigDecimal.ROUND_HALF_UP);
                        goodsCost = revenue.multiply(goodsCostRatio).setScale(6, BigDecimal.ROUND_HALF_UP);
//                    goodsCost = goodsCost.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
//                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                        itemMap.put("goodsCost", goodsCost.setScale(2, BigDecimal.ROUND_HALF_UP));


//                    refundRate = refundRate.divide(change, 4, BigDecimal.ROUND_HALF_UP);
                        BigDecimal refundRate = currentMonthRevenueZero ? currentMonthRevenue : refund.divide(currentMonthRevenue, 6, BigDecimal.ROUND_HALF_UP);
                        refund = revenue.multiply(refundRate).setScale(6, BigDecimal.ROUND_HALF_UP);
//                    refund = refund.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
//                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                        itemMap.put("refund", refund.setScale(2, BigDecimal.ROUND_HALF_UP));

//                    toolCostRatio = toolCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP);
                        BigDecimal toolCostRatio = currentMonthRevenueZero ? currentMonthRevenue : toolCost.divide(currentMonthRevenue, 6, BigDecimal.ROUND_HALF_UP);
                        toolCost = revenue.multiply(toolCostRatio).setScale(6, BigDecimal.ROUND_HALF_UP);
//                    toolCost = toolCost.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
//                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                        itemMap.put("toolCost", toolCost.setScale(2, BigDecimal.ROUND_HALF_UP));

//                    passCostRatio = passCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP);
                        BigDecimal passCostRatio = currentMonthRevenueZero ? currentMonthRevenue : passCost.divide(currentMonthRevenue, 6, BigDecimal.ROUND_HALF_UP);
                        passCost = revenue.multiply(passCostRatio).setScale(6, BigDecimal.ROUND_HALF_UP);
//                    passCost = passCost.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
//                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                        itemMap.put("passCost", passCost.setScale(2, BigDecimal.ROUND_HALF_UP));

//                    operateCostRatio = operateCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP);
                        BigDecimal operateCostRatio = currentMonthRevenueZero ? currentMonthRevenue : operateCost.divide(currentMonthRevenue, 6, BigDecimal.ROUND_HALF_UP);
                        operateCost = revenue.multiply(operateCostRatio).setScale(6, BigDecimal.ROUND_HALF_UP);
//                    operateCost = operateCost.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
//                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                        itemMap.put("operateCost", operateCost.setScale(2, BigDecimal.ROUND_HALF_UP));

                        BigDecimal zero = new BigDecimal(0.00);
                        boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;

                        itemMap.put("logisticCostRatio", revenueZero ? zero : logisticCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                        itemMap.put("goodsCostRatio", revenueZero ? zero : goodsCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                        itemMap.put("refundRate", revenueZero ? zero : refund.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                        itemMap.put("toolCostRatio", revenueZero ? zero : toolCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                        itemMap.put("passCostRatio", revenueZero ? zero : passCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                        itemMap.put("operateCostRatio", revenueZero ? zero : operateCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));

                        channelCost = String.valueOf(channelCost).equals("null") ? new BigDecimal(0.00) : channelCost;
                        cost = cost.add(channelCost);
                        itemMap.put("cost", cost.setScale(2, BigDecimal.ROUND_HALF_UP));

                        // ROAS
                        boolean costZero = cost.compareTo(BigDecimal.ZERO)==0;
                        itemMap.put("roas", costZero ? zero : revenue.divide(cost,2, RoundingMode.HALF_UP));

                        // 广告成本占比
                        itemMap.put("costProportion", revenueZero ? zero: (cost.divide(revenue,6, RoundingMode.HALF_UP)).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));


                        // 利润
                        // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
                        BigDecimal profit = revenue.subtract(cost).subtract(logisticCost).subtract(goodsCost).subtract(operateCost).subtract(refund).subtract(toolCost).subtract(passCost);


                        // 利润
                        BigDecimal profitRou = roundHalfUp(profit.setScale(2, BigDecimal.ROUND_HALF_UP));
                        itemMap.put("profit", profitRou);

                        // 利润率
                        // 利润/收入
                        // 判断被除数是否为零
                        BigDecimal profitRate = revenueZero ? zero : profitRou.divide(revenue, 6, BigDecimal.ROUND_HALF_UP);
                        itemMap.put("profitRate", roundHalfUp(profitRate.multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP)));

                        isCost = true;
                        break;
                    }
                }

            }
            // 全局成本
            if(!isCost){
                System.out.println("全局成本");

                for(Map<String, Object> overallMap : overallList){
//                    String costMonth = (String)overallMap.get("month");
//                    Long costItemId = (Long)overallMap.get("item_id");

//                    if(costItemId.equals(itemsId) && costMonth.equals(itemMonth)){
                    BigDecimal logisticCostRatio = (BigDecimal)overallMap.get("logisticCostRatio");
                    BigDecimal goodsCostRatio = (BigDecimal)overallMap.get("goodsCostRatio");
                    BigDecimal refundRate = (BigDecimal)overallMap.get("refundRate");
                    BigDecimal toolCostRatio = (BigDecimal)overallMap.get("toolCostRatio");
                    BigDecimal passCostRatio = (BigDecimal)overallMap.get("passCostRatio");
                    BigDecimal operateCostRatio = (BigDecimal)overallMap.get("operateCostRatio");

                    BigDecimal change = new BigDecimal(100);

                    BigDecimal logisticCost = revenue.multiply(logisticCostRatio.divide(change));

                    BigDecimal goodsCost = revenue.multiply(goodsCostRatio.divide(change));
                    BigDecimal refund = revenue.multiply(refundRate.divide(change));

                    BigDecimal toolCost = revenue.multiply(toolCostRatio.divide(change));

                    BigDecimal passCost = revenue.multiply(passCostRatio.divide(change));
                    BigDecimal operateCost = revenue.multiply(operateCostRatio.divide(change));

                    Date date2 = DateUtil.parseDateStr(itemMonth, "yyyy-MM");
                    int monthLastDay = DateUtil.getMonthLastDay(date2);
                    BigDecimal currentMonthDay = new BigDecimal(monthLastDay);
                    BigDecimal distanceDay = new BigDecimal(distanceOfTwoDate);
//                    System.out.println("logisticCost.divide(revenue, 4, BigDecimal.ROUND_HALF_UP)："+logisticCost.divide(revenue, 4, BigDecimal.ROUND_HALF_UP));

                    distanceDay = getDistanceDay(startTime, endTime, startMonth, endMonth, itemMonth);
                    System.out.println("distanceDay2："+distanceDay);

//                        logisticCost = logisticCost.divide(currentMonthDay).multiply(distanceDay);

//                        System.out.println("// logisticCost.divide(currentMonthDay)："+logisticCost.divide(currentMonthDay));
//                        if(revenue.compareTo(BigDecimal.ZERO)==0){
//                            logisticCost = logisticCost.divide(revenue);
//                        }

                    BigDecimal zero = new BigDecimal(0.00);

//                    logisticCost = logisticCost.divide(currentMonthDay, 4, BigDecimal.ROUND_HALF_UP).multiply(distanceDay);
                    System.out.println("logisticCost3："+logisticCost);

//                    itemMap.put("logisticCost", revenue.compareTo(BigDecimal.ZERO)==0 ? zero : logisticCost.divide(revenue, 4, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("logisticCost", logisticCost.setScale(2, BigDecimal.ROUND_HALF_UP));

//                    goodsCost = goodsCost.divide(currentMonthDay, 4, BigDecimal.ROUND_HALF_UP).multiply(distanceDay);
                    itemMap.put("goodsCost", goodsCost.setScale(2, BigDecimal.ROUND_HALF_UP));

//                    refund = refund.divide(currentMonthDay, 4, BigDecimal.ROUND_HALF_UP).multiply(distanceDay);
                    itemMap.put("refund", refund.setScale(2, BigDecimal.ROUND_HALF_UP));

//                    toolCost = toolCost.divide(currentMonthDay, 4, BigDecimal.ROUND_HALF_UP).multiply(distanceDay);
                    itemMap.put("toolCost", toolCost.setScale(2, BigDecimal.ROUND_HALF_UP));

//                    passCost = passCost.divide(currentMonthDay, 4, BigDecimal.ROUND_HALF_UP).multiply(distanceDay);
                    itemMap.put("passCost", passCost.setScale(2, BigDecimal.ROUND_HALF_UP));

//                    operateCost = operateCost.divide(currentMonthDay, 4, BigDecimal.ROUND_HALF_UP).multiply(distanceDay);
                    itemMap.put("operateCost", operateCost.setScale(2, BigDecimal.ROUND_HALF_UP));

//                    logisticCostRatio = revenue.compareTo(BigDecimal.ZERO)==0 ? zero : logisticCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(0, BigDecimal.ROUND_HALF_UP);

                    itemMap.put("logisticCostRatio", logisticCostRatio);
                    itemMap.put("goodsCostRatio", goodsCostRatio);
                    itemMap.put("refundRate", refundRate);
                    itemMap.put("toolCostRatio", toolCostRatio);
                    itemMap.put("passCostRatio", passCostRatio);
                    itemMap.put("operateCostRatio", operateCostRatio);

                    channelCost = String.valueOf(channelCost).equals("null") ? new BigDecimal(0.00) : channelCost;
                    cost = cost.add(channelCost);

                    itemMap.put("cost", cost.setScale(2, BigDecimal.ROUND_HALF_UP));

                    // ROAS
                    boolean isZero = cost.compareTo(BigDecimal.ZERO)==0;
                    BigDecimal roas = new BigDecimal(0.00);
                    if(isZero){
                        itemMap.put("roas", roas.setScale(2, BigDecimal.ROUND_HALF_UP));
                    }else{
                        itemMap.put("roas", revenue.divide(cost,2, RoundingMode.HALF_UP));
//                        roas = revenue.divide(cost, 2, RoundingMode.HALF_UP);
                    }

                    // 广告成本占比
                    boolean zero2 = revenue.compareTo(BigDecimal.ZERO)==0;
                    BigDecimal costProportion = new BigDecimal(0.00);
                    if(zero2){
                        itemMap.put("costProportion", costProportion.setScale(2, BigDecimal.ROUND_HALF_UP));
                    }else{
                        itemMap.put("costProportion", (cost.divide(revenue,6, RoundingMode.HALF_UP)).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
//                        costProportion = cost.divide(revenue,2, RoundingMode.HALF_UP);
                    }

                    // 利润
                    // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
                    BigDecimal profit = revenue.subtract(cost).subtract(logisticCost).subtract(goodsCost).subtract(operateCost).subtract(refund).subtract(toolCost).subtract(passCost);

                    // 利润率
                    // 利润/收入
                    boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;
                    BigDecimal profitRate = new BigDecimal(0.00);

                    // 利润
                    BigDecimal profitRou = roundHalfUp(profit.setScale(2, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("profit", profitRou);

                    // 判断被除数是否为零
                    profitRate = revenueZero ? profitRate : profitRou.divide(revenue, 6, BigDecimal.ROUND_HALF_UP);

                    BigDecimal profitRateRou = profitRate.multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP);

                    itemMap.put("profitRate", profitRateRou);

                    break;
//                    }
                }
            }
        }
    }




    public static void censusChannelCost1015(List<Map<String, Object>> tempList, List<Map<String, Object>> itemList, List<Map<String, Object>> costList
            , List<Map<String, Object>> overallList, String startTime, String endTime){

        int distanceOfTwoDate = DateUtil.getDistanceOfTwoDate(startTime, endTime); // 获取两个日期的天数
        System.out.println("distanceOfTwoDate："+distanceOfTwoDate);


        String startMonth = DateUtil.timestampToTime(DateUtil.parseDateStr(startTime, "yyyy-MM-dd").getTime());
        String endMonth = DateUtil.timestampToTime(DateUtil.parseDateStr(endTime, "yyyy-MM-dd").getTime());

        System.out.println("+++censusChannelCost itemList："+JSONArray.toJSONString(itemList));

        for(Map<String, Object> itemMap : itemList){
            String itemMonth = (String)itemMap.get("month");
//            Long itemsId = (Long)itemMap.get("itemsId");
            Integer channelId = (Integer)itemMap.get("channelId");
            BigDecimal revenue = (BigDecimal)itemMap.get("revenue");
            BigDecimal cost = (BigDecimal)itemMap.get("cost");

            System.out.println("-------->itemMap："+itemMap);

            boolean isCost = false;
            // 站点成本
            for(Map<String, Object> costMap : costList){
                System.out.println("-------->costMap："+costMap);
                String costMonth = (String)costMap.get("month");
//                Long costItemId = (Long)costMap.get("item_id");
                Integer costChannelId = (Integer)costMap.get("channelId");

                channelId = channelId == null ? 0 : channelId;
                costChannelId = costChannelId == null ? 0 : costChannelId;

                if(costChannelId.equals(channelId) && costMonth.equals(itemMonth)){
                    System.out.println("站点成本 channelId："+channelId);
                    System.out.println("站点成本 month："+costMonth);
                    BigDecimal logisticCost = (BigDecimal)costMap.get("logisticCost");
                    BigDecimal logisticCostRatio = (BigDecimal)costMap.get("logisticCostRatio");
                    BigDecimal goodsCost = (BigDecimal)costMap.get("goodsCost");
                    BigDecimal goodsCostRatio = (BigDecimal)costMap.get("goodsCostRatio");
                    BigDecimal refund = (BigDecimal)costMap.get("refund");
                    BigDecimal refundRate = (BigDecimal)costMap.get("refundRate");
                    BigDecimal toolCost = (BigDecimal)costMap.get("toolCost");
                    BigDecimal toolCostRatio = (BigDecimal)costMap.get("toolCostRatio");
                    BigDecimal passCost = (BigDecimal)costMap.get("passCost");
                    BigDecimal passCostRatio = (BigDecimal)costMap.get("passCostRatio");
                    BigDecimal operateCost = (BigDecimal)costMap.get("operateCost");
                    BigDecimal operateCostRatio = (BigDecimal)costMap.get("operateCostRatio");

                    int monthLastDay = DateUtil.getMonthLastDay(DateUtil.parseDateStr(costMonth, "yyyy-MM"));
                    BigDecimal currentMonthDay = new BigDecimal(monthLastDay);
                    BigDecimal distanceDay = new BigDecimal(distanceOfTwoDate);


                    distanceDay = getDistanceDay(startTime, endTime, startMonth, endMonth, costMonth);

/*
                    System.out.println("---------->costItemId："+costItemId);
                    System.out.println("distanceDay："+distanceDay);
                    System.out.println("startTime："+startTime);
                    System.out.println("endTime："+endTime);
                    System.out.println("startMonth："+startMonth);
                    System.out.println("endMonth："+endMonth);
                    System.out.println("<----------costMonth："+costMonth);
*/

                    System.out.println("<----------costMonth："+costMonth);
                    System.out.println("logisticCost："+logisticCost);
                    System.out.println("currentMonthDay："+currentMonthDay);
                    System.out.println("distanceDay："+distanceDay);
                    System.out.println("logisticCost："+logisticCost);
                    System.out.println("revenue："+revenue);
                    System.out.println("logisticCost.divide(revenue, 4, BigDecimal.ROUND_HALF_UP)："+logisticCost.divide(revenue, 4, BigDecimal.ROUND_HALF_UP));

                    BigDecimal change = new BigDecimal(100);


                    logisticCostRatio = logisticCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP);
                    logisticCost = revenue.multiply(logisticCostRatio).setScale(4, BigDecimal.ROUND_HALF_UP);
                    System.out.println("logisticCost11："+logisticCost);
                    logisticCost = logisticCost.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                    System.out.println("logisticCost22："+logisticCost);
                    itemMap.put("logisticCost", logisticCost);

                    goodsCostRatio = goodsCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP);
                    goodsCost = revenue.multiply(goodsCostRatio).setScale(4, BigDecimal.ROUND_HALF_UP);
                    goodsCost = goodsCost.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                    itemMap.put("goodsCost", goodsCost);

                    refundRate = refundRate.divide(change, 4, BigDecimal.ROUND_HALF_UP);
                    refund = revenue.multiply(refundRate).setScale(4, BigDecimal.ROUND_HALF_UP);
                    refund = refund.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                    itemMap.put("refund", refund);

                    toolCostRatio = toolCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP);
                    toolCost = revenue.multiply(toolCostRatio).setScale(4, BigDecimal.ROUND_HALF_UP);
                    toolCost = toolCost.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                    itemMap.put("toolCost", toolCost);

                    passCostRatio = passCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP);
                    passCost = revenue.multiply(passCostRatio).setScale(4, BigDecimal.ROUND_HALF_UP);
                    passCost = passCost.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                    itemMap.put("passCost", passCost);

                    operateCostRatio = operateCostRatio.divide(change, 4, BigDecimal.ROUND_HALF_UP);
                    operateCost = revenue.multiply(operateCostRatio).setScale(4, BigDecimal.ROUND_HALF_UP);
                    operateCost = operateCost.divide(currentMonthDay, 6, BigDecimal.ROUND_HALF_UP).multiply(distanceDay)
                            .setScale(2, BigDecimal.ROUND_HALF_UP);
                    itemMap.put("operateCost", operateCost);

                    itemMap.put("logisticCostRatio", logisticCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("goodsCostRatio", goodsCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("refundRate", refund.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("toolCostRatio", toolCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("passCostRatio", passCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("operateCostRatio", operateCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));

                    // ROAS
                    boolean zero = cost.compareTo(BigDecimal.ZERO)==0;
                    BigDecimal roas = new BigDecimal(0.00);
/*
                    if(zero){
                        itemMap.put("roas", roas.setScale(2, BigDecimal.ROUND_HALF_UP));
                    }else{

                        itemMap.put("roas", revenue.divide(cost,2, RoundingMode.HALF_UP));
                    }
*/

                    itemMap.put("roas", zero ? roas : revenue.divide(cost,2, RoundingMode.HALF_UP));

                    // 广告成本占比
                    boolean zero2 = revenue.compareTo(BigDecimal.ZERO)==0;
                    BigDecimal costProportion = new BigDecimal(0.00);

                    itemMap.put("costProportion", zero2 ? costProportion : (cost.divide(revenue,6, RoundingMode.HALF_UP)).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));

                    // 利润
                    // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
                    BigDecimal profit = revenue.subtract(cost).subtract(logisticCost).subtract(goodsCost).subtract(operateCost).subtract(refund).subtract(toolCost).subtract(passCost);

                    // 利润率
                    // 利润/收入
                    boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;
                    BigDecimal profitRate = new BigDecimal(0.00);

                    profitRate = revenueZero? profitRate : profit.divide(revenue, 2, BigDecimal.ROUND_HALF_UP);
                    itemMap.put("profitRate", roundHalfUp(profitRate.multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP)));
                    // 利润
                    itemMap.put("profit", roundHalfUp(profit.setScale(2, BigDecimal.ROUND_HALF_UP)));


                    isCost = true;
                    break;
                }
            }
            // 全局成本
            if(!isCost){
                System.out.println("全局成本："+channelId);

                for(Map<String, Object> overallMap : overallList){
//                    String costMonth = (String)overallMap.get("month");
//                    Long costItemId = (Long)overallMap.get("item_id");

//                    if(costItemId.equals(itemsId) && costMonth.equals(itemMonth)){
//                            BigDecimal logisticCost = (BigDecimal)overallMap.get("logisticCost");
                    BigDecimal logisticCostRatio = (BigDecimal)overallMap.get("logisticCostRatio");
//                            BigDecimal goodsCost = (BigDecimal)overallMap.get("goodsCost");
                    BigDecimal goodsCostRatio = (BigDecimal)overallMap.get("goodsCostRatio");
//                            BigDecimal refund = (BigDecimal)overallMap.get("refund");
                    BigDecimal refundRate = (BigDecimal)overallMap.get("refundRate");
//                            BigDecimal toolCost = (BigDecimal)overallMap.get("toolCost");
                    BigDecimal toolCostRatio = (BigDecimal)overallMap.get("toolCostRatio");
//                            BigDecimal passCost = (BigDecimal)overallMap.get("passCost");
                    BigDecimal passCostRatio = (BigDecimal)overallMap.get("passCostRatio");
//                            BigDecimal operateCost = (BigDecimal)overallMap.get("operateCost");
                    BigDecimal operateCostRatio = (BigDecimal)overallMap.get("operateCostRatio");

                    BigDecimal change = new BigDecimal(100);

                    BigDecimal logisticCost = revenue.multiply(logisticCostRatio.divide(change, 2));
                    BigDecimal goodsCost = revenue.multiply(goodsCostRatio.divide(change, 2));
                    BigDecimal refund = revenue.multiply(refundRate.divide(change, 2));
                    BigDecimal toolCost = revenue.multiply(toolCostRatio.divide(change, 2));
                    BigDecimal passCost = revenue.multiply(passCostRatio.divide(change, 2));
                    BigDecimal operateCost = revenue.multiply(operateCostRatio.divide(change, 2));

                    Date date2 = DateUtil.parseDateStr(itemMonth, "yyyy-MM");
                    System.out.println("date2date2date2:"+date2);
                    int monthLastDay = DateUtil.getMonthLastDay(date2);
                    BigDecimal currentMonthDay = new BigDecimal(monthLastDay);
                    BigDecimal distanceDay = new BigDecimal(distanceOfTwoDate);



                    System.out.println("<----------overallMonth...........");
                    System.out.println("logisticCost："+logisticCost);
                    System.out.println("currentMonthDay："+currentMonthDay);
                    System.out.println("distanceDay："+distanceDay);
                    System.out.println("logisticCost："+logisticCost);
                    System.out.println("revenue："+revenue);
//                    System.out.println("logisticCost.divide(revenue, 4, BigDecimal.ROUND_HALF_UP)："+logisticCost.divide(revenue, 4, BigDecimal.ROUND_HALF_UP));



                    distanceDay = getDistanceDay(startTime, endTime, startMonth, endMonth, itemMonth);

//                        logisticCost = logisticCost.divide(currentMonthDay).multiply(distanceDay);

                    System.out.println("// logisticCost："+logisticCost);
                    System.out.println("// revenue："+revenue);
//                        System.out.println("// logisticCost.divide(currentMonthDay)："+logisticCost.divide(currentMonthDay));
//                        if(revenue.compareTo(BigDecimal.ZERO)==0){
//                            logisticCost = logisticCost.divide(revenue);
//                        }

                    BigDecimal zero = new BigDecimal(0.00);

                    logisticCost = logisticCost.divide(currentMonthDay, 4, BigDecimal.ROUND_HALF_UP).multiply(distanceDay);

//                    itemMap.put("logisticCost", revenue.compareTo(BigDecimal.ZERO)==0 ? zero : logisticCost.divide(revenue, 4, BigDecimal.ROUND_HALF_UP));
//                    itemMap.put("logisticCost", logisticCost.setScale(2, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("logisticCost", zero);

                    goodsCost = goodsCost.divide(currentMonthDay, 4, BigDecimal.ROUND_HALF_UP).multiply(distanceDay);
                    itemMap.put("goodsCost", zero);

                    refund = refund.divide(currentMonthDay, 4, BigDecimal.ROUND_HALF_UP).multiply(distanceDay);
                    itemMap.put("refund", zero);

                    toolCost = toolCost.divide(currentMonthDay, 4, BigDecimal.ROUND_HALF_UP).multiply(distanceDay);
                    itemMap.put("toolCost", zero);

                    passCost = passCost.divide(currentMonthDay, 4, BigDecimal.ROUND_HALF_UP).multiply(distanceDay);
                    itemMap.put("passCost", zero);

                    operateCost = operateCost.divide(currentMonthDay, 4, BigDecimal.ROUND_HALF_UP).multiply(distanceDay);
                    itemMap.put("operateCost", zero);

                    logisticCostRatio = revenue.compareTo(BigDecimal.ZERO)==0 ? zero : logisticCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(0, BigDecimal.ROUND_HALF_UP);


//                    itemMap.put("logisticCostRatio", logisticCostRatio);
                    itemMap.put("logisticCostRatio", zero);

//                    itemMap.put("goodsCostRatio", revenue.compareTo(BigDecimal.ZERO)==0 ? zero : goodsCost.divide(revenue, 6, BigDecimal.ROUND_HALF_UP).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("goodsCostRatio", zero);
                    itemMap.put("refundRate", zero);
                    itemMap.put("toolCostRatio", zero);
                    itemMap.put("passCostRatio", zero);
                    itemMap.put("operateCostRatio", zero);


                    // ROAS
                    boolean isZero = cost.compareTo(BigDecimal.ZERO)==0;
                    BigDecimal roas = new BigDecimal(0.00);
                    if(isZero){
                        itemMap.put("roas", roas.setScale(2, BigDecimal.ROUND_HALF_UP));
                    }else{
//                        itemMap.put("roas", revenue.divide(cost,2, RoundingMode.HALF_UP));
                        itemMap.put("roas", revenue.divide(cost,2, RoundingMode.HALF_UP));
//                        roas = revenue.divide(cost, 2, RoundingMode.HALF_UP);
                    }

                    // 广告成本占比
                    boolean zero2 = revenue.compareTo(BigDecimal.ZERO)==0;
                    BigDecimal costProportion = new BigDecimal(0.00);
                    if(zero2){
                        itemMap.put("costProportion", costProportion.setScale(2, BigDecimal.ROUND_HALF_UP));
                    }else{
                        itemMap.put("costProportion", zero);
//                        costProportion = cost.divide(revenue,2, RoundingMode.HALF_UP);
                    }

                    // 利润
                    // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
                    BigDecimal profit = revenue.subtract(cost).subtract(logisticCost).subtract(goodsCost).subtract(operateCost).subtract(refund).subtract(toolCost).subtract(passCost);

                    // 利润率
                    // 利润/收入
                    boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;
                    BigDecimal profitRate = new BigDecimal(0.00);
                    if(revenueZero){
                    }else{
                        profitRate = profit.divide(revenue, 2, BigDecimal.ROUND_HALF_UP);
                    }
                    itemMap.put("profitRate", roundHalfUp(profitRate.multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP)));
                    // 利润
                    itemMap.put("profit", roundHalfUp(profit.setScale(2, BigDecimal.ROUND_HALF_UP)));


                    break;
//                    }
                }

            }
        }


        System.out.println("pre itemList："+ JSONArray.toJSONString(itemList));
        channelGroupBy(tempList, itemList);
        System.out.println("after tempList："+JSONArray.toJSONString(tempList));

    }

    public static BigDecimal roundHalfUp(BigDecimal value){
        return value.setScale(2, BigDecimal.ROUND_HALF_UP);
    }

    public static BigDecimal getDistanceDay(String startTime, String endTime, String startMonth, String endMonth, String costMonth){
        BigDecimal distanceDay = new BigDecimal(0);
        System.out.println("getDistanceDay costMonth："+costMonth);
        System.out.println("getDistanceDay startMonth："+startMonth);
        System.out.println("getDistanceDay startTime："+startTime);
        System.out.println("getDistanceDay endTime："+endTime);

        Date startDate = DateUtil.parseDate(startMonth, "yyyy-MM");
        Date endDate = DateUtil.parseDate(endMonth, "yyyy-MM");
        startMonth = DateUtil.formatDate(startDate, "yyyy-MM");
        endMonth = DateUtil.formatDate(endDate, "yyyy-MM");

        if(costMonth.equals(startMonth)){
            System.out.println("开始月份相等");
            Date start = DateUtil.parseDate(startTime, "yyyy-MM-dd");
            Date aMonthLastDay = DateUtil.parseDate(DateUtil.getAMonthLastDay(start.getTime()), "yyyy-MM-dd");
            String aMonthLastDayTime = DateUtil.formatDate(aMonthLastDay, "yyyy-MM-dd"); // 获取一个月最后一天

//            int distanceOfTwoDate2 = DateUtil.getDistanceOfTwoDate(startTime, endTime); // 获取两个日期的天数
            int distanceOfTwoDate2 = DateUtil.getDistanceOfTwoDate(startTime, aMonthLastDayTime); // 获取两个日期的天数
            distanceDay = new BigDecimal(distanceOfTwoDate2);

        }else if(costMonth.equals(endMonth)){
            System.out.println("结束月份相等");
            Date end = DateUtil.parseDate(endTime, "yyyy-MM-dd");
            Date aMonthFirstDay = DateUtil.parseDate(DateUtil.getAMonthFirstDay(end.getTime()), "yyyy-MM-dd");
            String aMonthFirstDayTime = DateUtil.formatDate(aMonthFirstDay, "yyyy-MM-dd"); // 获取一个月的第一天

            int distanceOfTwoDate2 = DateUtil.getDistanceOfTwoDate(aMonthFirstDayTime, endTime); // 获取两个日期的天数
            distanceDay = new BigDecimal(distanceOfTwoDate2);

        }else{
            System.out.println("月份不相等");
            Date itemMonthDate = DateUtil.parseDate(costMonth, "yyyy-MM");
//                        int currentMonthDayCount = DateUtil.getMonthLastDay(itemMonthDate); // 当月天数
            String aMonthFirstDayTime = DateUtil.formatDate(itemMonthDate, "yyyy-MM-dd"); // 获取一个月的第一天
            String aMonthLastDayTime = DateUtil.getAMonthLastDay(itemMonthDate.getTime()); // 获取一个月最后一天
            int distanceOfTwoDate2 = DateUtil.getDistanceOfTwoDate(aMonthFirstDayTime, aMonthLastDayTime); // 获取两个日期的天数
            distanceDay = new BigDecimal(distanceOfTwoDate2);
//            System.out.println("itemMonthDate："+itemMonthDate);
//            System.out.println("aMonthFirstDayTime："+aMonthFirstDayTime);
//            System.out.println("aMonthLastDayTime："+aMonthLastDayTime);
//            System.out.println("distanceOfTwoDate2："+distanceOfTwoDate2);

        }
        return distanceDay;
    }


    public static void channelGroupBy(List<Map<String, Object>> tempList, List<Map<String, Object>> itemList){
        System.out.println("////////itemList1："+JSONArray.toJSONString(itemList));
        System.out.println("////////tempList1："+JSONArray.toJSONString(tempList));

        for(Map<String, Object> itemMap : itemList){
//            Long itemsId = (Long)itemMap.get("itemsId");
            Integer channelId = (Integer)itemMap.get("channelId");
            String jobNumer = (String)itemMap.get("job_number");

            boolean isGather = false;

            for(Map<String, Object> tempMap : tempList){
//                Long tempItemsId = (Long)tempMap.get("itemsId");
                Integer tempChannelId = (Integer)tempMap.get("channelId");
                String tempJobNumer = (String)tempMap.get("job_number");

//                System.out.println("-------------->itemMap："+itemMap);
//                System.out.println("-------------->tempMap："+tempMap);
//                System.out.println("tempChannelId："+tempChannelId);
//                System.out.println("tempJobNumer："+tempJobNumer);
//                System.out.println("jobNumer："+jobNumer);

                jobNumer = StringUtils.isEmpty(jobNumer) ? "" : jobNumer;
                tempJobNumer = StringUtils.isEmpty(tempJobNumer) ? "" : tempJobNumer;
                channelId = channelId == null ? 0 : channelId;
                tempChannelId = tempChannelId == null ? 0 : tempChannelId;

                if(channelId.equals(tempChannelId)){
                    System.out.println("---汇总开始 channelId："+channelId + " jobNumer："+jobNumer);

                    BigDecimal tempRevenue = (BigDecimal)tempMap.get("revenue");
                    BigDecimal tempCost = (BigDecimal)tempMap.get("cost");


//                    System.out.println("itemMap itemMap itemMap："+itemMap);
                    BigDecimal revenue = (BigDecimal)itemMap.get("revenue");
                    BigDecimal cost = (BigDecimal)itemMap.get("cost");

                    revenue = tempRevenue.add(revenue);
                    tempMap.put("revenue", revenue.setScale(2, BigDecimal.ROUND_HALF_UP));
                    cost = tempCost.add(cost);
                    tempMap.put("cost", cost.setScale(2, BigDecimal.ROUND_HALF_UP));


                    BigDecimal change = new BigDecimal(100);

                    // ROAS
                    boolean zero = cost.compareTo(BigDecimal.ZERO)==0;
                    BigDecimal roas = new BigDecimal(0.00);
                    if(zero){
                        tempMap.put("roas", roas.setScale(2, BigDecimal.ROUND_HALF_UP));
                    }else{
                        tempMap.put("roas", revenue.divide(cost,2, RoundingMode.HALF_UP));
                    }

                    // 广告成本占比
                    boolean zero2 = revenue.compareTo(BigDecimal.ZERO)==0;
                    BigDecimal costProportion = new BigDecimal(0.00);
                    if(zero2){
                        tempMap.put("costProportion", costProportion.setScale(2, BigDecimal.ROUND_HALF_UP));
                    }else{
                        tempMap.put("costProportion", (cost.divide(revenue,6, RoundingMode.HALF_UP)).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
                    }

                    // 利润
                    // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
                    BigDecimal profit = revenue.subtract(cost);

                    // 利润率
                    // 利润/收入
                    boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;
                    BigDecimal profitRate = new BigDecimal(0.00);
                    if(revenueZero){
                    }else{
                        profitRate = profit.divide(revenue, 2, BigDecimal.ROUND_HALF_UP);
                    }
                    tempMap.put("profitRate", roundHalfUp(profitRate.multiply(change).setScale(0, BigDecimal.ROUND_HALF_UP)));
                    // 利润
                    tempMap.put("profit", roundHalfUp(profit.setScale(2, BigDecimal.ROUND_HALF_UP)));

                    isGather = true;
//                    break;

                }
            }

            // 如果没有汇总，就直接添加
            if(!isGather){
                tempList.add(itemMap);
            }
        }

        for(Map<String, Object> tempMap : tempList){
            BigDecimal revenue = (BigDecimal)tempMap.get("revenue");
            BigDecimal cost = (BigDecimal)tempMap.get("cost");

            tempMap.put("revenue", revenue.setScale(2, BigDecimal.ROUND_HALF_UP));
            tempMap.put("cost", cost.setScale(2, BigDecimal.ROUND_HALF_UP));
        }

    }


    public static void censusChannelCost(List<Map<String, Object>> tempList, List<Map<String, Object>> itemList, List<Map<String, Object>> costList
            , List<Map<String, Object>> channelRevenueList, String startTime, String endTime){

        int distanceOfTwoDate = DateUtil.getDistanceOfTwoDate(startTime, endTime); // 获取两个日期的天数
        System.out.println("distanceOfTwoDate："+distanceOfTwoDate);


        String startMonth = DateUtil.timestampToTime(DateUtil.parseDateStr(startTime, "yyyy-MM-dd").getTime());
        String endMonth = DateUtil.timestampToTime(DateUtil.parseDateStr(endTime, "yyyy-MM-dd").getTime());

        System.out.println("censusChannelCost start88888888888888888");

        System.out.println("censusChannelCost itemList："+JSONArray.toJSONString(itemList));

        System.out.println("censusChannelCost costList："+JSONArray.toJSONString(costList));

        System.out.println("censusChannelCost channelRevenueList："+JSONArray.toJSONString(channelRevenueList));


        for(Map<String, Object> itemMap : itemList){
            String itemMonth = (String)itemMap.get("month");
//            Long itemsId = (Long)itemMap.get("itemsId");
            Integer itemChannelIdInt = (Integer)itemMap.get("channelId");
            BigDecimal revenue = (BigDecimal)itemMap.get("revenue");
            BigDecimal cost = (BigDecimal)itemMap.get("cost");

            System.out.println("-------->itemMap："+itemMap);


            boolean isCost = false;
            // 站点成本
            for(Map<String, Object> costMap : costList){
                System.out.println("-------->costMap："+costMap);
                String costMonth = (String)costMap.get("month");
//                Long costItemId = (Long)costMap.get("item_id");
                Long costChannelId = (Long)costMap.get("channelId");
                BigDecimal channelCost = (BigDecimal)costMap.get("cost");

                Long itemChannelId = itemChannelIdInt == null ? 0 : Long.valueOf(itemChannelIdInt);
                costChannelId = costChannelId == null ? 0 : costChannelId;


                if(costChannelId.equals(Long.valueOf(itemChannelId)) && costMonth.equals(itemMonth)){

                    System.out.println("开始计算渠道成本 itemChannelId："+itemChannelId);
                    System.out.println("站点 month："+costMonth);
                    System.out.println("渠道 channelCost："+channelCost);

//                    BigDecimal logisticCost = (BigDecimal)costMap.get("logisticCost");
//                    BigDecimal logisticCostRatio = (BigDecimal)costMap.get("logisticCostRatio");




                    int monthLastDay = DateUtil.getMonthLastDay(DateUtil.parseDateStr(costMonth, "yyyy-MM"));
                    BigDecimal currentMonthDay = new BigDecimal(monthLastDay);

                    BigDecimal distanceDay = getDistanceDay(startTime, endTime, startMonth, endMonth, costMonth);

                    BigDecimal change = new BigDecimal(100);


                    // 获取当月收入
                    BigDecimal currentMonthRevenue = new BigDecimal(0.00);
                    for(Map<String, Object> channelRevenueMap : channelRevenueList){
//                        Long itemCostItemId = (Long)channelRevenueMap.get("items_id");
                        Integer currentChannelId = (Integer)channelRevenueMap.getOrDefault("channelId", 0);
                        String channelCostMonth = (String)channelRevenueMap.get("month");
                        if(itemChannelId.equals(Long.valueOf(currentChannelId)) && itemMonth.equals(channelCostMonth)){
                            currentMonthRevenue = (BigDecimal)channelRevenueMap.get("revenue");
                            System.out.println("获取当月收入 channelRevenueMap："+channelRevenueMap);
                            System.out.println("获取当月收入："+currentMonthRevenue);
                        }
                    }





                    System.out.println("站点成本 itemCost：" + cost);
                    System.out.println("站点成本 channelCost：" + channelCost);


                    cost = cost.add(channelCost);

                    System.out.println("站点成本 add cost：" + cost);
                    System.out.println("站点成本 currentMonthRevenue：" + currentMonthRevenue);

                    BigDecimal goodsCostRatio = cost.divide(currentMonthRevenue, 6, BigDecimal.ROUND_HALF_UP);
                    System.out.println("站点成本 revenue：" + revenue);
                    System.out.println("站点成本 goodsCostRatio：" + goodsCostRatio);
                    cost = revenue.multiply(goodsCostRatio).setScale(6, BigDecimal.ROUND_HALF_UP);
                    System.out.println("站点成本 cost：" + cost);

                    itemMap.put("cost", cost.setScale(2, BigDecimal.ROUND_HALF_UP));




                    // ROAS
                    boolean isZero = cost.compareTo(BigDecimal.ZERO)==0;
                    BigDecimal roas = new BigDecimal(0.00);
                    if(isZero){
                        itemMap.put("roas", roas.setScale(2, BigDecimal.ROUND_HALF_UP));
                    }else{
                        itemMap.put("roas", revenue.divide(cost,2, RoundingMode.HALF_UP));
//                        roas = revenue.divide(cost, 2, RoundingMode.HALF_UP);
                    }

                    // 广告成本占比
                    boolean zero2 = revenue.compareTo(BigDecimal.ZERO)==0;
                    BigDecimal costProportion = new BigDecimal(0.00);
                    if(zero2){
                        itemMap.put("costProportion", costProportion.setScale(2, BigDecimal.ROUND_HALF_UP));
                    }else{
                        itemMap.put("costProportion", (cost.divide(revenue,6, RoundingMode.HALF_UP)).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
//                        costProportion = cost.divide(revenue,2, RoundingMode.HALF_UP);
                    }

                    // 利润
                    // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
                    BigDecimal profit = revenue.subtract(cost);

                    // 利润率
                    // 利润/收入
                    boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;
                    BigDecimal profitRate = new BigDecimal(0.00);

                    // 利润
                    BigDecimal profitRou = roundHalfUp(profit.setScale(2, BigDecimal.ROUND_HALF_UP));
                    itemMap.put("profit", profitRou);

                    // 判断被除数是否为零
                    profitRate = revenueZero ? profitRate : profitRou.divide(revenue, 6, BigDecimal.ROUND_HALF_UP);

                    BigDecimal profitRateRou = profitRate.multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP);

                    System.out.println("--->profitRate："+profitRate);
                    System.out.println("change："+change);
                    System.out.println("profitRate.multiply(change)："+profitRate.multiply(change));
                    System.out.println("profitRateRou："+profitRateRou);

                    itemMap.put("profitRate", profitRateRou);


                    isCost = true;
                    break;

                }
            }
            // 全局成本
            if(!isCost){
                System.out.println("全局成本");


                BigDecimal change = new BigDecimal(100);

                // ROAS
                boolean isZero = cost.compareTo(BigDecimal.ZERO)==0;
                BigDecimal roas = new BigDecimal(0.00);
                if(isZero){
                    itemMap.put("roas", roas.setScale(2, BigDecimal.ROUND_HALF_UP));
                }else{
                    itemMap.put("roas", revenue.divide(cost,2, RoundingMode.HALF_UP));
//                        roas = revenue.divide(cost, 2, RoundingMode.HALF_UP);
                }

                // 广告成本占比
                boolean zero2 = revenue.compareTo(BigDecimal.ZERO)==0;
                BigDecimal costProportion = new BigDecimal(0.00);
                if(zero2){
                    itemMap.put("costProportion", costProportion.setScale(2, BigDecimal.ROUND_HALF_UP));
                }else{
                    itemMap.put("costProportion", (cost.divide(revenue,6, RoundingMode.HALF_UP)).multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP));
//                        costProportion = cost.divide(revenue,2, RoundingMode.HALF_UP);
                }

                // 利润
                // 收入-广告成本-物流成本-商品成本-运营成本-退款-工具成本-通道成本
                BigDecimal profit = revenue.subtract(cost);

                // 利润率
                // 利润/收入
                boolean revenueZero = revenue.compareTo(BigDecimal.ZERO)==0;
                BigDecimal profitRate = new BigDecimal(0.00);

                // 利润
                BigDecimal profitRou = roundHalfUp(profit.setScale(2, BigDecimal.ROUND_HALF_UP));
                itemMap.put("profit", profitRou);

                // 判断被除数是否为零
                profitRate = revenueZero ? profitRate : profitRou.divide(revenue, 6, BigDecimal.ROUND_HALF_UP);

                BigDecimal profitRateRou = profitRate.multiply(change).setScale(2, BigDecimal.ROUND_HALF_UP);

                System.out.println("--->profitRate："+profitRate);
                System.out.println("change："+change);
                System.out.println("profitRate.multiply(change)："+profitRate.multiply(change));
                System.out.println("profitRateRou："+profitRateRou);

                itemMap.put("profitRate", profitRateRou);


            }

        }


        System.out.println("pre itemList："+ JSONArray.toJSONString(itemList));
        channelGroupBy(tempList, itemList);
        System.out.println("after tempList："+JSONArray.toJSONString(tempList));

    }


    public static Long getItemsIdByName(List<Map<String, Object>> itemsList, String itemsName){
        for(Map<String, Object> itemsMap : itemsList){
            Long id = (Long)itemsMap.get("itemsId");
            String name = (String)itemsMap.get("itemsName");
            if(itemsName.trim().equals(name)){
                return id;
            }
        }
        return null;
    }

    public static Long getChannelIdByName(List<Map<String, Object>> itemsList, String channelName){
        for(Map<String, Object> itemsMap : itemsList){
            Integer id = (Integer)itemsMap.get("id");
            String name = (String)itemsMap.get("source_name");
            if(channelName.trim().equals(name)){
                return Long.valueOf(id);
            }
        }
        return null;
    }


}
