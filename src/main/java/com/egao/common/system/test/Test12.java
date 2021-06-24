package com.egao.common.system.test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class Test12 {

    public static void main(String[] args) throws Exception {


        JSONObject adAccountObject = JSONObject.parseObject("{\"data\":[{\"account_id\":\"1760902010716845\",\"campaigns\":{\"data\":[{\"name\":\"SYV2319-seeya -1025\",\"id\":\"23846415856620222\"},{\"name\":\"SYV2290-seeya -1025\",\"id\":\"23846415612230222\"},{\"name\":\"SK0451-3-seeya-1024-[142]\",\"id\":\"23846415527460222\"},{\"name\":\"SYQ0520-2-seeya\",\"id\":\"23846415495520222\"},{\"name\":\"SYV2230-YJW\",\"id\":\"23846200347810222\"},{\"name\":\"SYQ0520-4-YJW\",\"id\":\"23846154635420222\"},{\"name\":\"SYQ0520-3-YJW\",\"id\":\"23846131094800222\"},{\"name\":\"SYQ0520-2-YJW\",\"id\":\"23846068688100222\"},{\"name\":\"SYQ0520-YJW\",\"id\":\"23846054743420222\"},{\"name\":\"SYV2090-2-YJW\",\"id\":\"23846054703010222\"},{\"name\":\"SYV2090-YJW\",\"id\":\"23846048256560222\"},{\"name\":\"SYV2006-4-YJW\",\"id\":\"23846048180170222\"},{\"name\":\"SYV2073-4-YJW\",\"id\":\"23846048141240222\"},{\"name\":\"SYV2006-3-YJW\",\"id\":\"23845986235130222\"},{\"name\":\"SYV0786-YJW\",\"id\":\"23845977882840222\"},{\"name\":\"SYV2060-2-YJW\",\"id\":\"23845974951780222\"},{\"name\":\"SYV2073-3-YJW\",\"id\":\"23845974942430222\"},{\"name\":\"SYV2073-2-YJW\",\"id\":\"23845966607540222\"},{\"name\":\"SYV2073-YJW\",\"id\":\"23845964242430222\"},{\"name\":\"SYV2064-YJW\",\"id\":\"23845952529540222\"},{\"name\":\"SYV2061-YJW\",\"id\":\"23845946947130222\"},{\"name\":\"SYV2044-YJW\",\"id\":\"23845946683270222\"},{\"name\":\"SYV2060-YJW\",\"id\":\"23845937064710222\"},{\"name\":\"SYV2039-2-YJW\",\"id\":\"23845934886230222\"},{\"name\":\"SYV2006-2-YJW\",\"id\":\"23845932553730222\"},{\"name\":\"SYV2039-YJW\",\"id\":\"23845897863040222\"},{\"name\":\"SYV2006-YJW\",\"id\":\"23845895706570222\"},{\"name\":\"SYV1995-2-YJW\",\"id\":\"23845858930700222\"},{\"name\":\"SYQ0374-2-YJW\",\"id\":\"23845855993330222\"},{\"name\":\"SYV1995-YJW\",\"id\":\"23845855609250222\"},{\"name\":\"SYV1990-YJW\",\"id\":\"23845855534240222\"},{\"name\":\"SK0451-4-YJW\",\"id\":\"23845815318190222\"},{\"name\":\"sierpien-0826-2-YJW\",\"id\":\"23845812566350222\"},{\"name\":\"SK0451-3-YJW\",\"id\":\"23845812533850222\"},{\"name\":\"SYV1490-新-YJW\",\"id\":\"23845806458500222\"},{\"name\":\"SYV1891-2-YJW\",\"id\":\"23845806330330222\"},{\"name\":\"SK0451-2-YJW\",\"id\":\"23845806269450222\"},{\"name\":\"SYV1891-3-YJW\",\"id\":\"23845796759410222\"},{\"name\":\"sierpien-0826-2-YJW\",\"id\":\"23845791019410222\"},{\"name\":\"sierpien-0826-YJW\",\"id\":\"23845790748790222\"},{\"name\":\"YBL1875-YJW\",\"id\":\"23845789473460222\"},{\"name\":\"YBL1894-YJW\",\"id\":\"23845784916290222\"},{\"name\":\"SYV1920-YJW\",\"id\":\"23845774637500222\"},{\"name\":\"sierpien-0822-YJW\",\"id\":\"23845769646070222\"},{\"name\":\"SYV1921-YJW\",\"id\":\"23845768900950222\"},{\"name\":\"SYQ0374-YJW\",\"id\":\"23845768771600222\"},{\"name\":\"SYV1891-2-YJW\",\"id\":\"23845768698850222\"},{\"name\":\"SYV1685-自摄图-YJW\",\"id\":\"23845756689270222\"},{\"name\":\"SYV1891-2-YJW\",\"id\":\"23845750848120222\"},{\"name\":\"SYV1898-YJW\",\"id\":\"23845750803970222\"}],\"paging\":{\"next\":\"https://graph.facebook.com/v7.0/act_1760902010716845/campaigns?access_token=EAAH92JtasVMBAJ2iHbMXEdLwzMZAH2PidkMGwvQbhFZCZAAcPmUHOxfwaPfNg4M3vXCBonOVZAHLIrj7gdZCJqT9pQs8CAMGrBp7ECuNKOdFIO5txnP3UylNAI959oXBqp1hZAJloEBqSvVdt3hVhXYDu7WGdoZCgZCqrqX0PVE5LKKdGtlzQMxZBmrY8YWjQARUZD&pretty=1&fields=id%2Cname&limit=50&after=QVFIUnd4dFNNM1RoaEFuZAXJPcEctZAVJvLUJISXdNYlp2WW4wN0JFdXBWWTJLX2tqUDJXTkgyZA3ZAsZA0h0aGZA5X01hTFRWcTBTbWs0V1VxcE9iSUZAOekd6ME13\",\"cursors\":{\"before\":\"QVFIUlFfbnI2MFpKclN1MTNEcmoxdi1SUlNkNllYVFVvS3djVldUS25UUzl6T1BsQWlNS0QzcjlxZAlJDaDJPSWpzaDZARNUZANSWoyeXBwTlVKRm5EZAE1qUUln\",\"after\":\"QVFIUnd4dFNNM1RoaEFuZAXJPcEctZAVJvLUJISXdNYlp2WW4wN0JFdXBWWTJLX2tqUDJXTkgyZA3ZAsZA0h0aGZA5X01hTFRWcTBTbWs0V1VxcE9iSUZAOekd6ME13\"}}},\"name\":\"LYGM-MS-dafunia03\",\"id\":\"act_1760902010716845\"}],\"paging\":{\"cursors\":{\"before\":\"QVFIUlN1cU5WR2FQWE9ybGNXcEdJSHl3ek1kaFhuV3B6LS1PNmRWU1hNeHROM3JpVWNkOUE2NjdtSXVLN29EUXh6RHpDdlhZAbm96eHdZATExTa2R5NVFIemx3\",\"after\":\"QVFIUlN1cU5WR2FQWE9ybGNXcEdJSHl3ek1kaFhuV3B6LS1PNmRWU1hNeHROM3JpVWNkOUE2NjdtSXVLN29EUXh6RHpDdlhZAbm96eHdZATExTa2R5NVFIemx3\"}}}");

        JSONArray adAccountDataArr = adAccountObject.getJSONArray("data");

        // 遍历广告账户
        for(int i = 0; i < adAccountDataArr.size(); i++) {

            JSONObject adAccountObj = adAccountDataArr.getJSONObject(i);

//            JSONObject campaigns = adAccountObj.getJSONObject("campaigns");
//            JSONArray campaignsData = campaigns.getJSONArray("data");



            // 从广告名称上截取工号
            JSONObject campaigns = adAccountObj.getJSONObject("campaigns");
            JSONArray campaignsData = campaigns.getJSONArray("data");
            String campaignsDataStr = campaignsData.toJSONString();

            System.out.println("campaignsDataStr1："+campaignsDataStr);

            campaignsDataStr = campaignsDataStr.substring(1 , campaignsDataStr.length() - 1);



            System.out.println("campaignsDataStr2："+campaignsDataStr);
            System.out.println("campaignsDataStr.length()："+campaignsDataStr.length());


//            String campaignsStr = campaignsData.toJSONString();


            // 如果没有工号就跳过
            if (!campaignsDataStr.contains("[") || !campaignsDataStr.contains("]")) {
                continue;
            }
            // 截取工号
            String start = campaignsDataStr.substring(0, campaignsDataStr.indexOf("["));
            String end = campaignsDataStr.substring(0, campaignsDataStr.indexOf("]"));

            String jobNumber = campaignsDataStr.substring(start.length()+1, end.length());

            System.out.println("start.length()+1："+start.length()+1);
            System.out.println("end.length()："+end.length());
            System.out.println("jobNumber："+jobNumber);

        }





    }


}
