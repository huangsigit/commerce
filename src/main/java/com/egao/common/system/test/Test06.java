package com.egao.common.system.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.Constants;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.utils.HttpClientUtil;
import com.egao.common.core.utils.HttpClientsUtil;
import com.egao.common.core.utils.HttpUtil;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

// FB广告
public class Test06 {

    public static String GRAPH_URL = "https://graph.facebook.com/v7.0/";

    public static String ACCESS_TOKEN = "EAAH92JtasVMBAJ2iHbMXEdLwzMZAH2PidkMGwvQbhFZCZAAcPmUHOxfwaPfNg4M3vXCBonOVZAHLIrj7gdZCJqT9pQs8CAMGrBp7ECuNKOdFIO5txnP3UylNAI959oXBqp1hZAJloEBqSvVdt3hVhXYDu7WGdoZCgZCqrqX0PVE5LKKdGtlzQMxZBmrY8YWjQARUZD";

    public static String BUSINESS_ID = "144436283227029";

    private boolean environment = Constants.DEVELOPMENT_ENVIRONMENT;


    public static void main(String[] args) throws Exception {

        // 获取广告账户
        MultiValueMap<String, String> adAccountParams = new LinkedMultiValueMap<>();
        adAccountParams.add("access_token", ACCESS_TOKEN);
        adAccountParams.add("fields", "id,name,account_id,spend");

        String adAccountUrl = GRAPH_URL + BUSINESS_ID + "/client_ad_accounts?";
//        logger.info("adAccountParams："+adAccountParams);
        System.out.println("adAccountParams："+adAccountParams);


        String fields = "id,name,account_id,spend";

        adAccountUrl = adAccountUrl + "access_token=" + ACCESS_TOKEN + "&fields=" + fields;

//        logger.info("adAccountUrl："+adAccountUrl);
        System.out.println("adAccountUrl："+adAccountUrl);



        Map adAccountMap = new HashMap();
        adAccountMap.put("access_token", ACCESS_TOKEN);
        adAccountMap.put("fields", "id,name,account_id,spend,adcreatives{id,name,url_tags}");
        adAccountMap.put("access_token", ACCESS_TOKEN);

        HttpUtil httpUtil = new HttpUtil();

        String adAccountResult = httpUtil.doGet(adAccountUrl, adAccountMap);
        System.out.println("adAccountResult:"+adAccountResult);

        JSONObject adAccountObject = JSONObject.parseObject(adAccountResult);
        JSONArray adAccountDataArr = adAccountObject.getJSONArray("data");

        for(int i = 0; i < adAccountDataArr.size(); i++){


            JSONObject adAccountObj = adAccountDataArr.getJSONObject(i);
            String adAccountId = adAccountObj.getString("account_id");
            JSONObject adCreativesObj = adAccountObj.getJSONObject("adcreatives");
            String adCreativesStr = adCreativesObj.toJSONString();

            // 如果没有工号就跳过
            if(!adCreativesStr.contains("%5B") && !adCreativesStr.contains("%5D")){
                continue;
            }

            String start = adCreativesStr.substring(0, adCreativesStr.indexOf("%5B"));
            System.out.println("start：" + start);
            String end = adCreativesStr.substring(0, adCreativesStr.indexOf("%5D"));
            System.out.println("end：" + end);
            String jobNumber = adCreativesStr.substring(start.length()+3, end.length());


            // 获取广告系列
            String insightsUrl = GRAPH_URL + "act_" + adAccountId + "/insights?";
            /*
            MultiValueMap<String, String> campaignsParams = new LinkedMultiValueMap<>();
            campaignsParams.add("access_token", ACCESS_TOKEN);
            campaignsParams.add("time_range", "%7b'since':'2020-08-31','until':'2020-08-31'%7d");
            campaignsParams.add("fields", "account_id,spend,ad_id,campaign_id,date_start,date_stop,account_name,website_purchase_roas");*/


            String campaignsFields = "name,start_time,objective,status,spend";
//            campaignsUrl = campaignsUrl + "access_token=" + ACCESS_TOKEN + "&limit=" + 100 + "&field=" + campaignsFields;
//            logger.info("campaignsUrl:"+campaignsUrl);
            System.out.println("campaignsUrl:"+insightsUrl);

            Map insightsMap = new HashMap();
            insightsMap.put("access_token", ACCESS_TOKEN);
//            campaignsParams.put("time_range", "%7b'since':'2020-08-31','until':'2020-08-31'%7d");
            insightsMap.put("time_range", "{'since':'2020-09-01','until':'2020-09-01'}");
            insightsMap.put("fields", "account_id,spend,ad_id,campaign_id,date_start,date_stop,account_name,website_purchase_roas");

//            String campaignsResult = HttpUtil.get(campaignsUrl, campaignsParams);
            String insightsResult = httpUtil.doGet(insightsUrl, insightsMap);
            System.out.println(".............campaignsResult："+insightsResult);

            JSONObject insightsObjs = JSONObject.parseObject(insightsResult);
            JSONArray insightsDataArr = insightsObjs.getJSONArray("data");


            if(insightsDataArr.size() > 0){

                JSONObject insightsObj = insightsDataArr.getJSONObject(i);
                Double spend = insightsObj.getDouble("spend"); // 成本
                String date = insightsObj.getString("date_start");
                String accountName = insightsObj.getString("account_name");

                JSONArray purchaseRoasArr = insightsObj.getJSONArray("purchase_roas"); // 花费回报
                Double value = 0.00;
                if(purchaseRoasArr != null){
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

                BigDecimal revenue = new BigDecimal(spend*value).setScale(2, RoundingMode.HALF_UP);
                dataMap.put("revenue", String.format("%.2f", revenue)); // 收入
                dataMap.put("cost", spend); // 成本
                dataMap.put("type", 1); // 成本 ga0 fb1
                //                    map.put("create_time", DateUtil.timestampToTime(System.currentTimeMillis()-86400000, "yyyy-MM-dd"));
                dataMap.put("create_time", date);

                System.out.println("++++++++++++++++map:"+dataMap);

            }


            /*
            JSONObject adAccountObj = adAccountDataArr.getJSONObject(i);
            String adAccountId = adAccountObj.getString("account_id");


            // 获取广告系列
            String campaignsUrl = GRAPH_URL + "act_" + adAccountId + "/insights?";
            MultiValueMap<String, String> campaignsParams = new LinkedMultiValueMap<>();
            campaignsParams.add("access_token", ACCESS_TOKEN);
            campaignsParams.add("time_range", "{'since':'2020-08-31','until':'2020-08-31'}");
            campaignsParams.add("fields", "account_id,spend,ad_id,campaign_id,date_start,date_stop,account_name,website_purchase_roas");


            String campaignsFields = "account_id,spend,ad_id,campaign_id,date_start,date_stop,account_name,website_purchase_roas";
            Map map = new HashMap<>();
            JSONObject json = new JSONObject();

            json.put("until", "2020-08-31");
//            json.put("until", DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd"));
            json.put("since", "2020-08-31");
//            json.put("since", DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd"));

            String time_range = "%7b'until':'2020-08-31','since':'2020-08-31'%7d";
//            String time_range = "%7b\"until\":\"2020-09-01\",\"since\":\"2020-09-01\"%7d";
//            campaignsUrl = campaignsUrl + "access_token=" + ACCESS_TOKEN + "&limit=" + 100 + "&field=" + campaignsFields;
            campaignsUrl = campaignsUrl+ "time_range=" + time_range + "&access_token=" + ACCESS_TOKEN+ "&field=" + campaignsFields;
//            logger.info("campaignsUrl:"+campaignsUrl);
            System.out.println("campaignsUrl:"+campaignsUrl);

//            String campaignsResult = HttpUtil.get(campaignsUrl, campaignsParams);
            String campaignsResult = HttpUtil.getInstance().doGet(campaignsUrl, null);
//            String campaignsResult = HttpClientUtil.doGet(campaignsUrl);
//            String encode = URLEncoder.encode(campaignsUrl);

//            String campaignsResult = HttpClientUtil.doGet(campaignsUrl);
//            String campaignsResult = HttpClientsUtil.doGet(encode);

            map.put("", "");
//            campaignsUrl = "https://graph.facebook.com/v7.0/act_2523536311212491/insights?time_range={\"until\":\"2020-09-01\",\"since\":\"2020-09-01\"}&access_token=EAAH92JtasVMBAJ2iHbMXEdLwzMZAH2PidkMGwvQbhFZCZAAcPmUHOxfwaPfNg4M3vXCBonOVZAHLIrj7gdZCJqT9pQs8CAMGrBp7ECuNKOdFIO5txnP3UylNAI959oXBqp1hZAJloEBqSvVdt3hVhXYDu7WGdoZCgZCqrqX0PVE5LKKdGtlzQMxZBmrY8YWjQARUZD";
//            String campaignsResult = HttpClientsUtil.callByGet(campaignsUrl, "");

            System.out.println(".............campaignsResult："+campaignsResult);
*/



        }


    }
}
