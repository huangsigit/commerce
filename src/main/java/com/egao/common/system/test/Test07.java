package com.egao.common.system.test;

import com.egao.common.core.utils.DateUtil;

import java.math.BigDecimal;

public class Test07 {

    public static void main(String[] args) {

//        String countStr = "javascript:changeKeyValue(3);";
        String str = "utm_source=facebook.com&utm_medium=cpc&utm_campaign=SYQ0433-EUPL%E7%88%B1%E5%A5%BD-%5B124%5D&utm_content=2523536311212491\"url_tags\": \"utm_source=facebook.com&utm_medium=cpc&utm_campaign=SYQ0433-EUPL%E5%B7%A5%E5%95%86-%5B124%5D&utm_content=2523536311212491\"";

//        str = str.replaceAll("\\D", "");

        String start = str.substring(0, str.indexOf("%5B"));
        System.out.println("start：" + start);
        String end = str.substring(0, str.indexOf("%5D"));
        System.out.println("end：" + end);
        String jobNumber = str.substring(start.length()+3, end.length());

        System.out.println(jobNumber);

        Integer a = 100;
        Integer b = 100;
        System.out.println("a b：" + (a==b));

        Integer c = 1000;
        Integer d = 1000;
        System.out.println("c d：" + (c==d));


        int distanceOfTwoDate = DateUtil.getDistanceOfTwoDate("2020-09-03", "2020-09-03"); // 3
        System.out.println("distanceOfTwoDate："+distanceOfTwoDate);


    }
}
