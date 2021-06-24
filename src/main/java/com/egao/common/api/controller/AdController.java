package com.egao.common.api.controller;

/*

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.Constants;
import com.egao.common.core.UploadConstant;
import com.egao.common.core.annotation.OperLog;
import com.egao.common.core.utils.AnalyticsUtil;
import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.utils.HttpClientUtil;
import com.egao.common.core.utils.SecurityUtil;
import com.egao.common.core.web.JsonResult;
import com.egao.common.system.service.AdService;
import com.egao.common.system.service.CertificateService;
import com.egao.common.system.service.ItemsService;
import com.google.api.services.analytics.model.AccountSummary;
import com.google.api.services.analytics.model.ProfileSummary;
import com.google.api.services.analytics.model.WebPropertySummary;
import org.apache.tika.utils.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
@RequestMapping("/api")
public class AdController {


    @Autowired
    private AdService adService;

    @Autowired
    private CertificateService certificateService;

    private boolean environment = Constants.DEVELOPMENT_ENVIRONMENT;



    @OperLog(value = "Google广告", desc = "Google广告")
    @ResponseBody
    @RequestMapping(value = "/gaAd", produces = "application/json;charset=UTF-8")
    public String gaAd(HttpServletRequest request, @RequestParam(name = "keyword", required = false)String keyword) {

        System.out.println("Google广告 开始：" + DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH;mm:ss:SSS"));


        Map maps = new HashMap();
        List<Map<String, Object>> certificateList = certificateService.selectAllCertificate(maps);
//        List<Map<String, Object>> certificateList = null;
        System.out.println("certificateList："+certificateList);

        if(certificateList.size() > 0){
            Map<String, Object> certificateMap = certificateList.get(0);
            String serviceAccountId = (String)certificateMap.get("service_account_id");
            String path = (String)certificateMap.get("path");

//        String serviceAccountId = "ostudio01@ostudio01.iam.gserviceaccount.com";
//        String path = "2020/05/13/ostudio01-788809f30767.p12";

            File orgFile = new File(File.listRoots()[UploadConstant.UPLOAD_DIS_INDEX], UploadConstant.UPLOAD_DIR + path);

            System.out.println("serviceAccountId："+serviceAccountId);
            System.out.println("orgFile.getPath()："+orgFile.getPath());
            List<AccountSummary> itemList = AnalyticsUtil.getItems(serviceAccountId, orgFile.getPath());

            System.out.println("itemList:"+itemList);

            String yesterdayDate = DateUtil.timestampToTime(System.currentTimeMillis() - 86400000 - 86400000, "yyyy-MM-dd");
            System.out.println("yesterdayDate："+yesterdayDate);

            // 先删除数据再重新记录
            Map adMap = new HashMap();
            adMap.put("type", 0);
            adMap.put("create_time", DateUtil.timestampToTime(System.currentTimeMillis()-86400000 - 86400000, "yyyy-MM-dd"));
            adService.deleteByType(adMap);


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
                            System.out.println("--------------adData："+adData);

//                            logger.warning("adTasks adData：" + adData);

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

                                    String jobNumber = "";
                                    if(adName.contains("[")){
                                        // 截取广告名称中的工号
                                        String result2 = adName.substring(0, adName.indexOf("["));
                                        jobNumber = adName.substring(result2.length()+1, adName.length()-1);
                                    }

                                    map.put("job_number", jobNumber);
                                    map.put("ad_account", adAccount);
//                                map.put("ad_name", adName); // 有特殊字符，会报错
                                    map.put("ad_name", "");
                                    map.put("source", source);
                                    map.put("revenue", revenue);
                                    map.put("cost", cost);
                                    map.put("type", 0);
                                    map.put("create_time", DateUtil.timestampToTime(System.currentTimeMillis()-86400000 -86400000, "yyyy-MM-dd"));

                                    System.out.println("-----------------------map:"+map);

                                    adService.insertAd(map);
                                }
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            return "0";

                        }
                    }
                }
            }
        }

        System.out.println("Google广告 结束：" + DateUtil.timestampToTime(System.currentTimeMillis(), "yyyy-MM-dd HH;mm:ss:SSS"));
        return "1";



    }




}
*/
