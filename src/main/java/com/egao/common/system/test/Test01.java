package com.egao.common.system.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

public class Test01 {
    public static void main(String[] args) {

        String spendStr = "96.98";
        String valueStr = "1.693854";

//        String result = "{\"data\":[{\"account_id\":\"578026722972031\",\"campaign_id\":\"23844825691130787\",\"impressions\":\"7690\",\"spend\":\"32.67\",\"account_name\":\"FAYN-MS-dafunia01\",\"campaign_name\":\"SYV1556[110]\",\"date_start\":\"2020-06-06\",\"date_stop\":\"2020-06-06\"}],\"paging\":{\"cursors\":{\"before\":\"MAZDZD\",\"after\":\"MAZDZD\"}}}";
        String result = "{\"data\":[]}";

        JSONObject json = JSONObject.parseObject(result);
        JSONArray insightsDataArr = json.getJSONArray("data");


        for(int i = 0; i < insightsDataArr.size(); i++){

            JSONObject insightsDataObj = insightsDataArr.getJSONObject(0);
            Double spend = insightsDataObj.getDouble("spend");

            JSONArray purchaseRoasArr = insightsDataObj.getJSONArray("purchase_roas");

            if(purchaseRoasArr != null){

            }
            JSONObject purchaseRoasObj = purchaseRoasArr.getJSONObject(0);
            Double value = purchaseRoasObj.getDouble("value");

            System.out.println("spend:"+spend);
            System.out.println("value:"+value);
            Double revenue = spend*value;
            System.out.println("revenue:"+revenue);

            revenue = 23.96501465;

            BigDecimal revenue2 = new BigDecimal(revenue).setScale(2, RoundingMode.HALF_UP);
            System.out.println("revenue2:"+revenue2);

        }

        System.out.println(".............");

        Double spendD = 0.00;
        Double valueD = 0.00;

        Double r = spendD*valueD;

        System.out.println("r:"+new DecimalFormat("#.00").format(r));

        BigDecimal bg = new BigDecimal(r);
        double f1 = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        System.out.println(f1);
        System.out.println(String.format("%.2f", r));







    }
}
