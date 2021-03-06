package com.egao.common.core.scheduler;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.Cache;
import com.egao.common.core.Constants;
import com.egao.common.core.UploadConstant;
import com.egao.common.core.utils.AnalyticsUtil;
import com.egao.common.core.utils.CoreUtil;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.utils.HttpUtil;
import com.egao.common.system.service.AdService;
import com.egao.common.system.service.BusinessService;
import com.egao.common.system.service.CertificateService;
import com.egao.common.system.service.ItemsService;
import com.google.api.services.analytics.model.AccountSummary;
import com.google.api.services.analytics.model.ProfileSummary;
import com.google.api.services.analytics.model.WebPropertySummary;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import javax.annotation.Resource;
import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

import static com.egao.common.core.AdEnum.revenue;

@Component
@EnableScheduling
public class FacebookScheduler {

    Logger logger= Logger.getLogger(FacebookScheduler.class.getName());

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

    public static String GRAPH_URL = "https://graph.facebook.com/v7.0/";

    public static String ACCESS_TOKEN = "EAAH92JtasVMBAJ2iHbMXEdLwzMZAH2PidkMGwvQbhFZCZAAcPmUHOxfwaPfNg4M3vXCBonOVZAHLIrj7gdZCJqT9pQs8CAMGrBp7ECuNKOdFIO5txnP3UylNAI959oXBqp1hZAJloEBqSvVdt3hVhXYDu7WGdoZCgZCqrqX0PVE5LKKdGtlzQMxZBmrY8YWjQARUZD";

    public static String BUSINESS_ID = "144436283227029";

    private boolean environment = Constants.DEVELOPMENT_ENVIRONMENT;


    @Autowired
    private ItemsService itemsService;

    @Autowired
    private AdService adService;

    @Resource
    private BusinessService businessService;


//    @Scheduled(fixedRate = 3600000)
    // ????????????
    @Scheduled(cron = "1 1 9,16 * * ?", zone = "Asia/Shanghai")
    public void adTasks() {

        if(environment){
            return;
        }

        try {
            // ??????????????????

/*

            Map adMap = new HashMap();
            adMap.put("create_time", DateUtil.timestampToTime(System.currentTimeMillis()-86400000, "yyyy-MM-dd"));
            adMap.put("type", Constants.FB_AD);
//            adMap.put("items_id", businessId);
            adService.deleteByType(adMap);
//                adService.deleteByItemsId(adMap);
*/


            Map map = new HashMap();
            map.put("page", 0);
            map.put("rows", 1000);
            List<Map<String, Object>> businessList = businessService.selectBusiness(map);
            for(Map<String, Object> businessMap : businessList){
                System.out.println("??????for??????");
                String businessId = (String)businessMap.get("business_id");
                String businessName = (String)businessMap.get("business_name");

//                String adAccountUrl = GRAPH_URL + BUSINESS_ID + "/client_ad_accounts?";
                String adAccountUrl = GRAPH_URL + businessId + "/client_ad_accounts?";

                logger.info("adAccountUrl???"+adAccountUrl);
                System.out.println("adAccountUrl???"+adAccountUrl);


                Map adAccountMap = new HashMap();
                adAccountMap.put("access_token", ACCESS_TOKEN);
//                adAccountMap.put("fields", "id,name,account_id,spend,adcreatives{id,name,url_tags}");
//                adAccountMap.put("fields", "id,name,account_id,spend,campaigns.limit(30){id,name}");
                adAccountMap.put("fields", "id,name,account_id,spend,account_status,campaigns{name}");
                adAccountMap.put("account_status", "1");
                adAccountMap.put("limit", "300");

                HttpUtil httpUtil = new HttpUtil();

                String adAccountResult = httpUtil.doGet(adAccountUrl, adAccountMap);
                System.out.println("adAccountResult:"+adAccountResult);

                JSONObject adAccountObject = JSONObject.parseObject(adAccountResult);
                JSONArray adAccountDataArr = adAccountObject.getJSONArray("data");

                String startTime = DateUtil.timestampToTime(System.currentTimeMillis()-86400000, "yyyy-MM-dd");

/*
                Map adMap = new HashMap();
                adMap.put("create_time", startTime);
                adMap.put("type", Constants.FB_AD);
                adMap.put("items_id", businessId);
                adService.deleteByType(adMap);
//                adService.deleteByItemsId(adMap);
*/


                for(int i = 0; i < adAccountDataArr.size(); i++) {
                    System.out.println("??????for??????");

                    JSONObject adAccountObj = adAccountDataArr.getJSONObject(i);
                    String adAccountId = adAccountObj.getString("account_id");
                    String accountStatusStr = adAccountObj.getString("account_status");


                    // ??????
                    Map adMap2 = new HashMap();
                    adMap2.put("create_time", startTime);
                    adMap2.put("type", Constants.FB_AD);
//                    adMap.put("items_id", businessId);
                    adMap2.put("ad_account", adAccountId);
//                    System.out.println("delete adMap???"+adMap2);
//                    adService.deleteByType(adMap);
//                    adService.deleteByItemsId(adMap);
                    adService.deleteByAdAccount(adMap2);


                    Integer accountStatus = Integer.parseInt(accountStatusStr);
                    if(accountStatus != 1){ // ?????????????????????????????????1
                        continue;
                    }

                    // ??????????????????????????????
                    JSONObject campaigns = adAccountObj.getJSONObject("campaigns");

                    // ????????????campaigns??????
                    if(campaigns == null || campaigns.size() <= 0){
                        continue;
                    }

                    JSONArray campaignsData = campaigns.getJSONArray("data");

                    // ????????????????????????
                    if(campaignsData == null || campaignsData.size() <= 0){
                        continue;
                    }
                    String campaignsDataStr = campaignsData.toJSONString();
                    campaignsDataStr = campaignsDataStr.substring(1 , campaignsDataStr.length() - 1);
                    // ???????????????????????????
                    if (!campaignsDataStr.contains("[") || !campaignsDataStr.contains("]")) {
                        continue;
                    }
                    // ????????????
                    String start = campaignsDataStr.substring(0, campaignsDataStr.indexOf("["));
                    String end = campaignsDataStr.substring(0, campaignsDataStr.indexOf("]"));
                    String jobNumber = campaignsDataStr.substring(start.length() + 1, end.length());
                    boolean isInteger = CoreUtil.isInteger(jobNumber);

                    // ????????????????????????
                    if(StringUtils.isEmpty(jobNumber) || jobNumber.length() < 1 || jobNumber.length() > 10 || !isInteger){
                        continue;
                    }

                    System.out.println("jobNumber???"+jobNumber);

                    // ??????????????????URL
                    String insightsUrl = GRAPH_URL + "act_" + adAccountId + "/insights?";

                    System.out.println("campaignsUrl:" + insightsUrl);

                    String insightsFields = "account_id,spend,ad_id,campaign_id,date_start,date_stop,account_name,website_purchase_roas";
                    Map insightsMap = new HashMap();
                    insightsMap.put("access_token", ACCESS_TOKEN);
                    //            campaignsParams.put("time_range", "%7b'since':'2020-08-31','until':'2020-08-31'%7d");
                    insightsMap.put("time_range", "{'since':'"+startTime+"','until':'"+startTime+"'}");
                    insightsMap.put("fields", insightsFields);

                    System.out.println("777insightsMap???"+insightsMap);

                    //            String campaignsResult = HttpUtil.get(campaignsUrl, campaignsParams);
                    String insightsResult = httpUtil.doGet(insightsUrl, insightsMap);
                    System.out.println(".............campaignsResult???" + insightsResult);

                    JSONObject insightsObjs = JSONObject.parseObject(insightsResult);
                    JSONArray insightsDataArr = insightsObjs.getJSONArray("data");


                    if (insightsDataArr != null && insightsDataArr.size() > 0) {

                        JSONObject insightsObj = insightsDataArr.getJSONObject(0);
                        Double spend = insightsObj.getDouble("spend"); // ??????
                        String date = insightsObj.getString("date_start");
                        String accountName = insightsObj.getString("account_name");

                        JSONArray purchaseRoasArr = insightsObj.getJSONArray("purchase_roas"); // ????????????
                        Double value = 0.00;
                        if (purchaseRoasArr != null) {
                            JSONObject purchaseRoasObj = purchaseRoasArr.getJSONObject(0);
                            value = purchaseRoasObj.getDouble("value");
                        }

                        Map dataMap = new HashMap<>();
//                        dataMap.put("items_id", adAccountId);
                        dataMap.put("items_id", businessId);
                        //            map.put("job_number", jobNumber);
                        dataMap.put("job_number", jobNumber);
                        dataMap.put("ad_account", adAccountId);
                        //            map.put("ad_name", campaignsName);
                        dataMap.put("ad_name", accountName);
                        dataMap.put("source", "facebook.com/cpc"); // ???????????? ??????

                        BigDecimal revenue = new BigDecimal(spend * value).setScale(2, RoundingMode.HALF_UP);
//                    dataMap.put("revenue", String.format("%.2f", revenue)); // FB???????????????
                        dataMap.put("revenue", new BigDecimal(0.00)); // ??????
                        dataMap.put("cost", spend); // ??????
                        dataMap.put("type", 1); // ?????? ga0 fb1
                        dataMap.put("create_time", date);

                        System.out.println("++++++++++++++++map:" + dataMap);

                        adService.insertAd(dataMap);

                    }
                }

            }



        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    // ??????1??????????????????
    // ????????????
//    @Scheduled(cron = "0 5 * * * ?", zone = "Asia/Shanghai")
    @Scheduled(fixedRate = 3600000)
    public void itemTasks() {

        int item = 2;
//        if(item == 1){
        if(environment){
            return;
        }

        System.out.println("FacebookScheduler adTasks ???????????????????????????" + DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH;mm:ss:SSS"));

        logger.warning("FacebookScheduler adTasks ???????????????????????????" + dateFormat.format(new Date()));

        try {

            Map bMap = new HashMap();
            bMap.put("page", 0);
            bMap.put("rows", 1000);
            List<Map<String, Object>> businessList = businessService.selectBusiness(bMap);

            List<Map<String, Object>> adAccountAllList = itemsService.selectAllItemsByType(1);

            List adAccountList = new ArrayList();
            for(Map<String, Object> businessMap : businessList){
                System.out.println("??????for??????");
                String businessId = (String)businessMap.get("business_id");
                String businessName = (String)businessMap.get("business_name");

                MultiValueMap<String, String> params = new LinkedMultiValueMap<>();
//                params.add("access_token", ACCESS_TOKEN);
//                params.add("fields", "id,name,account_id,account_status");
//                params.add("limit", "300");

//            String url = GRAPH_URL + BUSINESS_ID + "/client_ad_accounts?";
//            String url = "https://graph.facebook.com/v7.0/144436283227029/client_ad_accounts?";
                String adAccountUrl = GRAPH_URL + businessId + "/client_ad_accounts?";

                System.out.println("adAccountUrl???"+adAccountUrl);


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
                        if(accountStatus != 1){ // ?????????????????????????????????1
                            continue;
                        }

                        Map map = new HashMap();
                        map.put("id", id);
                        map.put("business_id", businessId);
                        map.put("name", name);
                        map.put("type", 1);
                        System.out.println("delete map???"+map);

                        boolean existAdAccount = isExistAdAccount(adAccountAllList, id);
                        if(!existAdAccount){
                            try {
                                // ???????????????????????????????????????
                                itemsService.insertItems(map);
                                adAccountList.add(map);

                                System.out.println("?????? ?????? map???"+map);

                            } catch (Exception e) {
                                System.out.println("?????? ?????? map???"+map + " e???"+e.getMessage());
                            }
                        }else{
                            System.out.println("????????? id???"+id+" name???"+name);
                        }

                    }
                    // ???????????????
//                    Cache.setAdAccountList(adAccountList);
                }
                System.out.println("Cache.getAdAccountList()???"+Cache.getAdAccountList());

            }
            // ???????????????
            Cache.setAdAccountList(adAccountList);



        } catch (Exception e) {
            e.printStackTrace();
        }

        logger.warning("??????????????????");
        System.out.println("???????????????????????????" + DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH;mm:ss:SSS"));

    }


    // ??????????????????????????????????????????
    public boolean isExistAdAccount(List<Map<String, Object>> adAccountList, Long id){

        for(Map<String, Object> adAccountMap : adAccountList){
            Long listId = (Long)adAccountMap.get("value");
            if(listId.equals(id)){
                return true;
            }
        }

        return false;
    }



}
