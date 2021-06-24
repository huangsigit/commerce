package com.egao.common.system.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.utils.AnalyticsUtil;
import com.egao.common.core.utils.Base64;
import com.egao.common.core.utils.DateUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class Test14 {

    public static void main(String[] args) throws Exception {

//        String adData = AnalyticsUtil.getAdData("206036759", "ostudio01@ostudio01.iam.gserviceaccount.com"
        String adData = AnalyticsUtil.getAdData("200153889", "ostudio01@ostudio01.iam.gserviceaccount.com"
                    , "F:/Program/Document/Need/Analytics/ostudio01-788809f30767.p12", "2020-11-10", "2020-11-10");

        System.out.println("adData："+adData);


        System.out.println("adData："+adData);

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
//                map.put("profiles_id", profileId);
                map.put("profiles_id", null);

                String jobNumber = "";
                if(adName.contains("[")){
                    // 截取广告名称中的工号
                    String result2 = adName.substring(0, adName.indexOf("["));
                    jobNumber = adName.substring(result2.length()+1, adName.length()-1);
                }

                map.put("job_number", jobNumber);
                map.put("ad_account", adAccount);
                map.put("ad_name", Base64.encode(adName, "utf-8")); // 有特殊字符，特殊处理
                map.put("source", source);
                map.put("revenue", revenue);
                map.put("cost", cost);
                map.put("type", 0);
                map.put("create_time", DateUtil.timestampToTime(System.currentTimeMillis()-86400000, "yyyy-MM-dd"));

                System.out.println("GoogleSchedule map:"+map);

                try {
//                    adService.insertAd(map);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }
        }




    }





}
