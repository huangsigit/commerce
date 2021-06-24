package com.egao.common.core.utils;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.AnalyticsScopes;
import com.google.api.services.analytics.model.AccountSummaries;
import com.google.api.services.analytics.model.AccountSummary;
import com.google.api.services.analytics.model.GaData;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class AnalyticsUtil {

    static Logger logger= Logger.getLogger(AnalyticsUtil.class.getName());

    static String profileId = "ga:206036759";
    //    String profileId = "ga:212681555";
    static String serviceAccountId = "ostudio01@ostudio01.iam.gserviceaccount.com";
    //    String serviceAccountId = "ostudio04-160@ostudio04.iam.gserviceaccount.com";
//    String serviceAccountId = "smdgiwow@ostudio04.iam.gserviceaccount.com";
//    String serviceAccountId = "smdgiwow@gmail.com";
//    String serviceAccountId = "ostudio001@encoded-adviser-269904.iam.gserviceaccount.com";
    String filePath = "D:/Program/WorkspaceI/FinanceServer/target/classes/WEB-INF/classes/ostudio01-788809f30767.p12";
//    String filePath = "D:/Program/WorkspaceI/FinanceServer/target/classes/WEB-INF/classes/ostudio04-daf6db9b3f2b.p12";
//    String filePath = "D:/Program/WorkspaceI/FinanceServer/target/classes/WEB-INF/classes/ostudio04-fd7ce6ee56ad.p12";

//    String p12File = "ostudio01-788809f30767.p12";


    //     * gaData.getColumnHeaders() 查询出的结果的类的集合
//     *  gaData.getRows() 查询出的数据的集合
    public static String getAdData(String profileId, String serviceAccountId, String p12File, String startTime, String endTime) {


        try {
            // 初始化 GA服务
            Analytics analytics = initializeAnalytics(serviceAccountId, p12File);

            logger.warning("getAdData analytics：" + analytics);
            System.out.println("analytics："+analytics);

            // 获取查询参数
            Analytics.Data.Ga.Get get = analytics
                    .data()
                    .ga()
    //                .get("ga:212681555","30daysAgo",
    //                .get("ga:206036759","30daysAgo","yesterday",
//                    .get("ga:"+profileId,"2020-03-12","2020-03-22",
                    .get("ga:"+profileId, startTime, endTime,
    //                        "ga:adwordsCustomerID");
    //                        "ga:adwordsCustomerID");
    //                        "ga:users,ga:goalValueAll,ga:goalCompletionsAll,ga:goalConversionRateAll,ga:adClicks,ga:adCost,ga:transactions,ga:transactionRevenue,ga:newUsers");
    //                        "ga:users");
                            "ga:transactionRevenue,ga:adCost");
//                        "ga:totalValue");
//                        "ga:cohortActiveUsers");
//                        "ga:utm_term");
//                        "ga:utm_term");
            // 查询的列
//        get.setDimensions("ga:pageTitle,ga:pagePath");
//        get.setDimensions("ga:keyword");
//        get.setDimensions("ga:adwordsCampaignID");
//        get.setDimensions("ga:adwordsCampaignID,ga:adwordsCustomerID,ga:campaign");
//        get.setDimensions("ga:campaign");
            get.setDimensions("ga:campaign,ga:adwordsCustomerID,ga:sourceMedium");
//        get.setDimensions("ga:dimension1");
//        get.getDimensions()
            // 查询类型
//        get.setMetrics("ga:bounceRate").setMetrics("ga:goalValueAll"); // 跳出率
//        get.setMetrics("ga:goalValueAll"); // 目标价值
//        get.setMetrics("ga:adCost"); // 广告成本 764.56
//        get.setMetrics("ga:adwordsCustomerID"); // 广告账户ID
//        get.setMetrics("ga:users"); // 广告账户ID
//        get.setMetrics("ga:sourceMedium"); // 广告账户ID


//            get.setFilters("ga:adwordsCustomerID!=(not set)");
//        get.setFilters("ga:campaign=@["); // 包含[ga:campaign


            //排序
//        get.setSort("-ga:uniquePageviews");
            //过滤条件
            //dataQuery.setFilters(filter);

            GaData data = get.execute();
            return data.toString();
        } catch (Exception e) {
            e.printStackTrace();
            logger.warning("getAdData e：" + e);
        }
        return null;
    }


    //     * 初始化GA 事务信息
//     *
//     * @return
//     * @throws Exception
    private static Analytics initializeAnalytics(String serviceAccountId, String p12File) throws Exception {
        HttpTransport httpTransport = GoogleNetHttpTransport
                .newTrustedTransport();
        JsonFactory jsonFactory = new JacksonFactory();
        // 作用域信息
        List<String> list = new ArrayList<String>();
        list.add(AnalyticsScopes.ANALYTICS_READONLY); //访问域设置
//        String paths = this.getClass().getClassLoader().getResource("/").getPath();
//        System.out.println("paths："+paths);
//        String paths = analytics3.class.getClassLoader().getResource("/").getPath();

//        URL resource = AnalyticsUtil.class.getResource("/");
//        System.out.println("resource2："+resource);
//        System.out.println("resource.getPath："+resource.getPath());
//        String resourcePath = resource.getPath().substring(1, resource.getPath().length());

        //证书存放地址
        // 凭证证书信息
        GoogleCredential credential = new GoogleCredential.Builder()
                .setTransport(httpTransport)
                .setJsonFactory(jsonFactory)
//                .setServiceAccountId("ostudio002@encoded-adviser-269904.iam.gserviceaccount.com")
//                .setServiceAccountId("ostudio002@developer.gserviceaccount.com")
//                .setServiceAccountId("ostudio04-160@ostudio04.iam.gserviceaccount.com")
//                .setServiceAccountId("ostudio05@encoded-adviser-269904.iam.gserviceaccount.com")
                .setServiceAccountId(serviceAccountId)
//                .setServiceAccountId("117984266266458224175")
                .setServiceAccountPrivateKeyFromP12File(
//                        new File("D:/Program/WorkspaceI/FinanceServer/target/classes/WEB-INF/classes/" + "/encoded-adviser-269904-50895c4eea60.json"))
//                        new File("D:/Program/WorkspaceI/FinanceServer/target/classes/WEB-INF/classes/" + "/ostudio04-daf6db9b3f2b.p12"))
//                        new File("D:/Program/WorkspaceI/FinanceServer/target/classes/WEB-INF/classes/" + "/encoded-adviser-269904-a6a06fa2dfe5.p12"))
//                        new File(filePath))
//                        new File(resourcePath + "/analytics/" + p12File))
                        new File(p12File))
                .setServiceAccountScopes(list).build();

        logger.warning("Analytics credential：" + credential);

        credential.refreshToken();

        // 初始化访问事务信息
        return new Analytics.Builder(httpTransport, jsonFactory, null)
                .setHttpRequestInitializer(credential).build();
    }

    public static List<AccountSummary> getItems(String serviceAccountId, String p12File) {

        try {
            // 初始化 GA服务
            Analytics analytics = initializeAnalytics(serviceAccountId, p12File);
            Analytics.Management.AccountSummaries accountSummaries = analytics.management().accountSummaries();
            AccountSummaries execute = accountSummaries.list().execute();
            System.out.println("execute："+execute);

            List<AccountSummary> itemList = execute.getItems();
            System.out.println("itemList："+itemList);


            return itemList;

        } catch (Exception e) {
            e.printStackTrace();
            logger.warning("getItems e：" + e);
        }

        return null;
    }



}
