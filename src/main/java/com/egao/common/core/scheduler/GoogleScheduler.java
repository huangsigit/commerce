package com.egao.common.core.scheduler;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.Cache;
import com.egao.common.core.Constants;
import com.egao.common.core.UploadConstant;
import com.egao.common.core.utils.AnalyticsUtil;
import com.egao.common.core.utils.Base64;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.system.service.AdService;
import com.egao.common.system.service.CertificateService;
import com.egao.common.system.service.ItemsService;
import com.google.api.services.analytics.model.AccountSummary;
import com.google.api.services.analytics.model.ProfileSummary;
import com.google.api.services.analytics.model.WebPropertySummary;
//import com.ostudio.ew.common.utils.AnalyticsUtil;
//import com.ostudio.ew.common.utils.DateUtil;
//import com.ostudio.ew.common.utils.HttpUtil;
//import com.ostudio.ew.common.utils.PropertiesUtil;
//import com.ostudio.ew.system.service.AdService;
//import com.ostudio.ew.system.service.CertificateService;
//import com.ostudio.ew.system.service.ItemsService;
import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

@Component
@EnableScheduling
public class GoogleScheduler {

    Logger logger= Logger.getLogger(GoogleScheduler.class.getName());


    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");

//    @Autowired
//    private StringRedisTemplate redisTemplate;

    public static String TOKEN_URL = "https://accounts.google.com/o/oauth2/iframerpc";
    public static String SITE_URL = "https://content.googleapis.com/analytics/v3/management/accountSummaries";

    @Autowired
    private ItemsService itemsService;

    @Autowired
    private AdService adService;

    @Autowired
    private CertificateService certificateService;

    private boolean environment = Constants.DEVELOPMENT_ENVIRONMENT;


    // ???????????? ??????1??????????????????
//    @Scheduled(cron = "1 0 * * * ?", zone = "Asia/Shanghai")

//    @Scheduled(fixedRate = 30*60*1000)
    @Scheduled(fixedRate = 2*60*60*1000)
    public void itemsTasks() {

//        if(false){
        if(environment){
            return;
        }

        logger.info("itemsTasks startTime:"+DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss:SSS"));
        try {
            System.out.println("startTime:"+System.currentTimeMillis());

            Map maps = new HashMap();
            List<Map<String, Object>> certificateList = certificateService.selectAllCertificate(maps);


            System.out.println("certificateList:"+certificateList);
            if(certificateList.size() > 0){
                Map<String, Object> certificateMap = certificateList.get(0);
                String serviceAccountId = (String)certificateMap.get("service_account_id");
                String path = (String)certificateMap.get("path");

                File orgFile = new File(File.listRoots()[UploadConstant.UPLOAD_DIS_INDEX], UploadConstant.UPLOAD_DIR + path);

                System.out.println("orgFile???"+orgFile);
                logger.info("itemsTasks orgFile:" + orgFile);

                List<AccountSummary> itemList = AnalyticsUtil.getItems(serviceAccountId, orgFile.getPath());

                logger.info("itemsTasks itemList:" + itemList);
                System.out.println("itemsTasks itemList:" + itemList);

                if(itemList != null && itemList.size() > 0){
                    itemsService.deleteByType(0);
                    List itemsList = new ArrayList();
                    for(AccountSummary item : itemList){
                        String id = item.getId();
                        String name = item.getName();

                        Map map = new HashMap();
                        map.put("id", id);
                        map.put("business_id", null);
                        map.put("name", name);
                        map.put("type", 0);

                        itemsList.add(map);

                        itemsService.insertItems(map);
                    }
                    // ???????????????
                    Cache.setItemsList(itemsList);
                }
                System.out.println("Cache.getItemsList()???"+Cache.getItemsList());

            }

            logger.info("itemsTasks endTime:"+System.currentTimeMillis());
        } catch (Exception e) {
            e.printStackTrace();
            logger.warning("itemsTasks error:"+e);
        }
    }


    // ??????0?????????
    // ??????????????????
    @Scheduled(cron = "1 8 0 * * ?", zone = "Asia/Shanghai")
//    @Scheduled(fixedRate = 60000000)
    public void adTasks() {

        if(environment){
            return;
        }
        System.out.println("???????????????????????????" + DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH;mm:ss:SSS"));

        logger.warning("?????????????????? ???????????????????????????" + dateFormat.format(new Date()));


        try {
            Map maps = new HashMap();
            List<Map<String, Object>> certificateList = certificateService.selectAllCertificate(maps);
            System.out.println("certificateList???"+certificateList);

            if(certificateList.size() > 0){
                Map<String, Object> certificateMap = certificateList.get(0);
                String serviceAccountId = (String)certificateMap.get("service_account_id");
                String path = (String)certificateMap.get("path");

    //        String serviceAccountId = "ostudio01@ostudio01.iam.gserviceaccount.com";
    //        String path = "2020/05/13/ostudio01-788809f30767.p12";

                File orgFile = new File(File.listRoots()[UploadConstant.UPLOAD_DIS_INDEX], UploadConstant.UPLOAD_DIR + path);

                System.out.println("serviceAccountId???"+serviceAccountId);
                System.out.println("orgFile.getPath()???"+orgFile.getPath());
                List<AccountSummary> itemList = AnalyticsUtil.getItems(serviceAccountId, orgFile.getPath());

                System.out.println("itemList:"+itemList);

                String yesterdayDate = DateUtil.timestampToTime(System.currentTimeMillis() - 86400000, "yyyy-MM-dd");
                System.out.println("yesterdayDate???"+yesterdayDate);

                // ??????????????????????????????
                Map adMap = new HashMap();
                adMap.put("type", 0);
                adMap.put("create_time", DateUtil.timestampToTime(System.currentTimeMillis()-86400000, "yyyy-MM-dd"));
                adService.deleteByType(adMap);


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
                                // ???????????????????????????
                                String adData = AnalyticsUtil.getAdData(String.valueOf(profileId), serviceAccountId
                                        , orgFile.getPath(), yesterdayDate, yesterdayDate);
                                System.out.println("adData???"+adData);

                                JSONObject adObj = JSONObject.parseObject(adData);
                                JSONArray rowsArr = adObj.getJSONArray("rows");

                                JSONObject profileInfoObj = adObj.getJSONObject("profileInfo");
                                Long accountId = profileInfoObj.getLong("accountId");

                                if(rowsArr != null && rowsArr.size() > 0){
                                    Map map = new HashMap<>();
                                    for(int i = 0; i < rowsArr.size(); i++){
                                        JSONArray adArr = rowsArr.getJSONArray(i);
                                        String adName = adArr.getString(0); // ????????????
                                        String adAccount = adArr.getString(1); // ????????????ID
                                        String source = adArr.getString(2); // ????????????
                                        String revenue = adArr.getString(3); // ??????
                                        String cost = adArr.getString(4); // ????????????

                                        map.put("items_id", accountId);
                                        map.put("profiles_id", profileId);

                                        String jobNumber = "";
                                        if(adName.contains("[")){
                                            // ??????????????????????????????
                                            String result2 = adName.substring(0, adName.indexOf("["));
                                            jobNumber = adName.substring(result2.length()+1, adName.length()-1);
                                        }

                                        map.put("job_number", jobNumber);
                                        map.put("ad_account", adAccount);
                                        map.put("ad_name", Base64.encode(adName, "utf-8")); // ??????????????????????????????
                                        map.put("source", source);
                                        map.put("revenue", revenue);
                                        map.put("cost", cost);
                                        map.put("type", 0);
                                        map.put("create_time", DateUtil.timestampToTime(System.currentTimeMillis()-86400000, "yyyy-MM-dd"));

                                        System.out.println("GoogleSchedule map:"+map);

                                        try {
                                            adService.insertAd(map);
                                        } catch (Exception e) {
                                            e.printStackTrace();
                                        }

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

    public void adTasks2() {

    }




}
