package com.egao.common.system.test;

import com.egao.common.core.utils.DateUtil;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Test10 {

    public static void main(String[] args) throws Exception {


        String startTime = "2020-10-02";
        String endTime = "2020-10-08";
        String startMonth = "2020-10-02 00:00:00";
        String endMonth = "2020-10-08 00:00:00";
        String itemMonth = "2020-10";

        BigDecimal distanceDay = getDistanceDay(startTime, endTime, startMonth, endMonth, itemMonth);
        System.out.println("distanceDay2："+distanceDay);




    }

    public static BigDecimal getDistanceDay(String startTime, String endTime, String startMonth, String endMonth, String costMonth){
        BigDecimal distanceDay = new BigDecimal(0);
        System.out.println("getDistanceDay costMonth："+costMonth);
        System.out.println("getDistanceDay startMonth："+startMonth);
        System.out.println("getDistanceDay startTime："+startTime);
        System.out.println("getDistanceDay endTime："+endTime);

        Date startDate = DateUtil.parseDate(startMonth, "yyyy-MM");
        Date endDate = DateUtil.parseDate(endMonth, "yyyy-MM");
        startMonth = DateUtil.formatDate(startDate, "yyyy-MM");
        endMonth = DateUtil.formatDate(endDate, "yyyy-MM");

        if(costMonth.equals(startMonth)){
            System.out.println("开始月份相等");
            Date start = DateUtil.parseDate(startTime, "yyyy-MM-dd");
            Date aMonthLastDay = DateUtil.parseDate(DateUtil.getAMonthLastDay(start.getTime()), "yyyy-MM-dd");
//            String aMonthLastDayTime = DateUtil.formatDate(aMonthLastDay, "yyyy-MM-dd"); // 获取一个月最后一天

            int distanceOfTwoDate2 = DateUtil.getDistanceOfTwoDate(startTime, endTime); // 获取两个日期的天数
            distanceDay = new BigDecimal(distanceOfTwoDate2);

        }else if(costMonth.equals(endMonth)){
            System.out.println("结束月份相等");
            Date end = DateUtil.parseDate(endTime, "yyyy-MM-dd");
            Date aMonthFirstDay = DateUtil.parseDate(DateUtil.getAMonthFirstDay(end.getTime()), "yyyy-MM-dd");
            String aMonthFirstDayTime = DateUtil.formatDate(aMonthFirstDay, "yyyy-MM-dd"); // 获取一个月的第一天

            int distanceOfTwoDate2 = DateUtil.getDistanceOfTwoDate(aMonthFirstDayTime, endTime); // 获取两个日期的天数
            distanceDay = new BigDecimal(distanceOfTwoDate2);

        }else{
            System.out.println("月份不相等");
            Date itemMonthDate = DateUtil.parseDate(costMonth, "yyyy-MM");
//                        int currentMonthDayCount = DateUtil.getMonthLastDay(itemMonthDate); // 当月天数
            String aMonthFirstDayTime = DateUtil.formatDate(itemMonthDate, "yyyy-MM-dd"); // 获取一个月的第一天
            String aMonthLastDayTime = DateUtil.getAMonthLastDay(itemMonthDate.getTime()); // 获取一个月最后一天
            int distanceOfTwoDate2 = DateUtil.getDistanceOfTwoDate(aMonthFirstDayTime, aMonthLastDayTime); // 获取两个日期的天数
            distanceDay = new BigDecimal(distanceOfTwoDate2);
//            System.out.println("itemMonthDate："+itemMonthDate);
//            System.out.println("aMonthFirstDayTime："+aMonthFirstDayTime);
//            System.out.println("aMonthLastDayTime："+aMonthLastDayTime);
//            System.out.println("distanceOfTwoDate2："+distanceOfTwoDate2);

        }
        return distanceDay;
    }



}
