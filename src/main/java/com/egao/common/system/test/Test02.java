package com.egao.common.system.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.utils.HttpUtil;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class Test02 {

    public static String GRAPH_URL = "https://graph.facebook.com/v7.0/";

    public static String ACCESS_TOKEN = "EAAH92JtasVMBAJ2iHbMXEdLwzMZAH2PidkMGwvQbhFZCZAAcPmUHOxfwaPfNg4M3vXCBonOVZAHLIrj7gdZCJqT9pQs8CAMGrBp7ECuNKOdFIO5txnP3UylNAI959oXBqp1hZAJloEBqSvVdt3hVhXYDu7WGdoZCgZCqrqX0PVE5LKKdGtlzQMxZBmrY8YWjQARUZD";
    public static void main(String[] args) {
/*

        String campaignsId = "23845558170190318";

        String insightsUrl = GRAPH_URL + campaignsId + "/insights?";
        String data = DateUtil.timestampToTime(System.currentTimeMillis() - 86400000, "yyyy-MM-dd");
        String time_range = "{'since':'"+data+"','until':'"+data+"'}";
//        insightsUrl = insightsUrl + "access_token=" + ACCESS_TOKEN + "&time_range={'since':'2020-06-14','until':'2020-06-14'}";
//        insightsUrl = "https://graph.facebook.com/v7.0/144436283227029/client_ad_accounts?access_token=xxx&fields=id,name,account_id,spend";
        System.out.println("insightsUrl:"+insightsUrl);

        Map<String,String> params = new HashMap<>();

        params.put("access_token", ACCESS_TOKEN);
        params.put("time_range", time_range);


//        String insightsResult = HttpUtil.get(insightsUrl, null);
        String insightsResult = HttpUtil.getInstance().doGet(insightsUrl, params);


        System.out.println("insightsResult:"+insightsResult);
*/


        String datas = DateUtil.timestampToTime(System.currentTimeMillis() - 86400000, "yyyy-MM-dd");
        System.out.println("datas:" + datas);


    }
}
