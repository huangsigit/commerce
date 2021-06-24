package com.egao.common.system.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.utils.DateUtil;

import java.math.BigDecimal;
import java.util.*;

public class Test11 {

    public static void main(String[] args) throws Exception {

        JSONArray itemList = JSONArray.parseArray("[{\"revenue\":1862.69,\"cost\":0.00,\"month\":\"2020-10\",\"itemsId\":118674272,\"job_number\":\"\",\"sourceName\":\"facebook\",\"channelId\":187},{\"revenue\":891.91,\"cost\":554.64,\"month\":\"2020-10\",\"itemsId\":118674272,\"job_number\":\"006\",\"sourceName\":\"google\",\"channelId\":201}]");
        JSONArray costList = JSONArray.parseArray("[{\"toolCostRatio\":5.29,\"itemsName\":\"Regosoul\",\"item_id\":164690674,\"operateCostRatio\":1.06,\"goodsCostRatio\":10.59,\"logisticCostRatio\":12.71,\"passCost\":60.00,\"cost_id\":22,\"month\":\"0000-08\",\"passCostRatio\":6.35,\"itemsId\":164690674,\"refundRate\":1.06,\"goodsCost\":100.00,\"toolCost\":50.00,\"logisticCost\":120.00,\"refund\":10.00,\"operateCost\":10.00},{\"toolCostRatio\":12.25,\"itemsName\":\"Regosoul\",\"item_id\":164690674,\"operateCostRatio\":2.45,\"goodsCostRatio\":24.50,\"logisticCostRatio\":29.40,\"passCost\":60.00,\"cost_id\":23,\"month\":\"2020-08\",\"passCostRatio\":14.70,\"itemsId\":164690674,\"refundRate\":2.45,\"goodsCost\":100.00,\"toolCost\":50.00,\"logisticCost\":120.00,\"refund\":10.00,\"operateCost\":10.00},{\"cost_id\":25,\"itemsName\":\"Sonsoulier\",\"month\":\"2020-08\",\"item_id\":134723789,\"itemsId\":134723789,\"goodsCost\":160.00,\"passCost\":60.00,\"toolCost\":310.00,\"logisticCost\":120.00,\"refund\":260.00,\"operateCost\":210.00},{\"cost_id\":27,\"itemsName\":\"Obangbag\",\"month\":\"2020-09\",\"item_id\":118674272,\"itemsId\":118674272,\"goodsCost\":100.00,\"passCost\":300.00,\"toolCost\":200.00,\"logisticCost\":100.00,\"refund\":150.00,\"operateCost\":150.00}]");

        System.out.println("itemList："+itemList.get(0));

//        int differMonths = DateUtil.compareDate("2020-07", "2020-10", 1); 3
//        int differMonths = DateUtil.compareDate("2020-07", "2020-09", 1);
//        System.out.println("differMonths："+differMonths);


        for(int i = 0; i < itemList.size(); i++){
            JSONObject itemMap = itemList.getJSONObject(i);
            String itemMonth = (String)itemMap.get("month");
            Integer itemItemsId = (Integer)itemMap.get("itemsId");

            boolean isCost = false;
            boolean isAdjoinCost = false;
            for(int j = 0; j < costList.size(); j++){
                JSONObject costMap = costList.getJSONObject(i);
                String costMonth = (String)costMap.get("month");
                Long costItemId = (Long)costMap.get("item_id");
                if(itemItemsId.equals(costItemId) && itemMonth.equals(costMonth)){
                    System.out.println("站点成本");


                    isCost = true;
                    break;
                }
                if(itemItemsId.equals(costItemId)){
                    isAdjoinCost = true;
                    System.out.println("相邻成本 go");

                }

            }


            // 取最邻近目前日期的月份，或者说最大月份
            if(!isAdjoinCost){
                System.out.println("相邻成本");
//                Map<String, Integer> cMap = new HashMap();
//                cMap.put("differMonth", 999);
//                cMap.put("costIndex", 0);
                for(int j = 0; j < costList.size(); j++){
                    JSONObject costMap = costList.getJSONObject(i);
                    String costMonth = (String)costMap.get("month");
                    Long costItemId = (Long)costMap.get("item_id");

                    if(itemItemsId.equals(costItemId)){
//                        Integer differMonth = cMap.get("differMonth");

//                        int differMonths = DateUtil.compareDate(itemMonth, costMonth, j);
//                        System.out.println("differMonths："+differMonths);

                        break;
                    }

                }


            }


            // 全局成本
            if(!isCost){
                System.out.println("全局成本");
            }



        }




    }


}
