package com.egao.common.system.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test13 {

    public static void main(String[] args) throws Exception {


        String str = "{\"account_id\":\"579206719450167\",\"campaigns\":{\"data\":[{\"name\":\"10/31 拓-SYQ0739-7-ZXM-12.5pm-F45-64\",\"id\":\"23846048689300694\"},{\"name\":\"10/31 拓-SYQ0739-7-ZXM-12.5pm-F45-64\",\"id\":\"23846048689270694\"},{\"name\":\"10/31 拓-SYQ0739-EU-ZXM-12pm-F45-64\",\"id\":\"23846048689260694\"},{\"name\":\"10/31 拓-SYQ0739-EU-ZXM-12pm-F45-64\",\"id\":\"23846048689220694\"},{\"name\":\"10/31 拓  SYV2316 WW ZXM [121 2pm F45-64\",\"id\":\"23846048668710694\"},{\"name\":\"10/31拓  SYV2316 EU ZXM [121 1.5pm F45-64\",\"id\":\"23846048668670694\"},{\"name\":\"10/31 拓  SYV2316 WW ZXM [121 1.5pm F45-64\",\"id\":\"23846048668650694\"},{\"name\":\"10/31拓  SYV2316 7 ZXM [121 1.5pm F45-64\",\"id\":\"23846048668640694\"},{\"name\":\"10/31拓  SYV2316 EU ZXM [121 2pm F45-64\",\"id\":\"23846048668630694\"},{\"name\":\"10/27 拓-SYQ0739-DE-ZXM-2.5pm-All45-64\",\"id\":\"23846022461830694\"}],\"paging\":{\"next\":\"https://graph.facebook.com/v7.0/act_579206719450167/campaigns?access_token=EAAH92JtasVMBAJ2iHbMXEdLwzMZAH2PidkMGwvQbhFZCZAAcPmUHOxfwaPfNg4M3vXCBonOVZAHLIrj7gdZCJqT9pQs8CAMGrBp7ECuNKOdFIO5txnP3UylNAI959oXBqp1hZAJloEBqSvVdt3hVhXYDu7WGdoZCgZCqrqX0PVE5LKKdGtlzQMxZBmrY8YWjQARUZD&pretty=1&fields=name&limit=10&after=QVFIUkdNTHJvb3VLa1VTcFRMLUQ5cmlKUUVjSGF0SjJJdDRiRmswQ1JDSVpCYzlhUnRQTEIxWV9WdE9OSkxldUt1LXpoWU5XamF2cTZAWQ1dxbEREZAFd6UUp3\",\"cursors\":{\"before\":\"QVFIUnN2UjRzSjkweWZAraFNsNGp0eVBHSlNwRmdaT290YzhnRzR5N3djQnpmaVg5VGpWQmdiTzZAaQWdNX1FEdjM1dG45aGk4UndWSXBoTTUtLVBTdFN6QnVn\",\"after\":\"QVFIUkdNTHJvb3VLa1VTcFRMLUQ5cmlKUUVjSGF0SjJJdDRiRmswQ1JDSVpCYzlhUnRQTEIxWV9WdE9OSkxldUt1LXpoWU5XamF2cTZAWQ1dxbEREZAFd6UUp3\"}}},\"name\":\"LYGM-MS-Eugenstern-10\",\"id\":\"act_579206719450167\",\"account_status\":1}";

        JSONObject adAccountObj = JSONObject.parseObject(str);
//        JSONObject adAccountObj = adAccountDataArr.getJSONObject(i);
        // 从广告名称上截取工号
        JSONObject campaigns = adAccountObj.getJSONObject("campaigns");

        // 如果没有campaigns数组
        if(campaigns == null || campaigns.size() <= 0){
            System.out.println("没有数组");
        }

        System.out.println("campaigns："+campaigns);
        JSONArray campaignsData = campaigns.getJSONArray("data");

        // 如果没有广告名称
        if(campaignsData == null || campaignsData.size() <= 0){
            System.out.println("没有广告名称");
        }
        String campaignsDataStr = campaignsData.toJSONString();
        campaignsDataStr = campaignsDataStr.substring(1 , campaignsDataStr.length() - 1);
        // 如果没有工号就跳过
        if (!campaignsDataStr.contains("[") || !campaignsDataStr.contains("]")) {
            System.out.println("没有工号");
        }

        if (!campaignsDataStr.contains("]")) {
            System.out.println("没有工号2");
        }


        System.out.println("campaignsDataStr："+campaignsDataStr);



    }


    public static boolean isInteger(String str) {
        Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
        return pattern.matcher(str).matches();
    }




}
