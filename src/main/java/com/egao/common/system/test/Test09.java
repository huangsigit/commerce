package com.egao.common.system.test;

import com.egao.common.core.utils.DateUtil;
import com.egao.common.core.web.JsonResult;
import org.apache.commons.lang3.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Test09 {

    public static void main(String[] args) throws Exception {

        String searchTime = "2020-08-01 - 2020-08-31";

        String startTime = StringUtils.substringBefore(searchTime, " - ");
        // 获取7天前日期
        startTime = StringUtils.isEmpty(startTime) ? DateUtil.timestampToTime(System.currentTimeMillis() - 86400000 * 7, "yyyy-MM-dd") : startTime;

        String endTime = StringUtils.substringAfter(searchTime, " - ");
        // 获取昨天日期
        endTime = StringUtils.isEmpty(endTime) ? DateUtil.timestampToTime(System.currentTimeMillis() - 86400000, "yyyy-MM-dd") : endTime;

        if(StringUtils.isNotEmpty(startTime) && StringUtils.isNotEmpty(endTime)){
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd") ; //使用了默认的格式创建了一个日期格式化对象。
            Date startDate = dateFormat.parse(startTime); //注意:指定的字符串格式必须要与SimpleDateFormat的模式要一致。
            Date endDate = dateFormat.parse(endTime); //注意:指定的字符串格式必须要与SimpleDateFormat的模式要一致。
            if(startDate.getTime() > endDate.getTime()){
//                return JsonResult.error("开始日期不能大于结束日期...");
            }
        }

        Date start = DateUtil.parseDate(startTime, "yyyy-MM-dd");
        Date aMonthLastDay = DateUtil.parseDate(DateUtil.getAMonthLastDay(start.getTime()), "yyyy-MM-dd"); // 获取一个月最后一天
        String aMonthLastDayTime = DateUtil.formatDate(aMonthLastDay, "yyyy-MM-dd");

        Date end = DateUtil.parseDate(endTime, "yyyy-MM-dd");
        Date aMonthFirstDay = DateUtil.parseDate(DateUtil.getAMonthFirstDay(end.getTime()), "yyyy-MM-dd"); // 获取一个月的第一天
        String aMonthFirstDayTime = DateUtil.formatDate(aMonthFirstDay, "yyyy-MM-dd");
        System.out.println("aMonthLastDayTime：" + aMonthLastDayTime);
        System.out.println("aMonthFirstDayTime：" + aMonthFirstDayTime);

        int distanceOfTwoDate = DateUtil.getDistanceOfTwoDate(startTime, endTime); // 获取两个日期的天数
        System.out.println("distanceOfTwoDate："+distanceOfTwoDate);

/*
        Calendar cale = null;
        cale = Calendar.getInstance();
        cale.setTime(aMonthLastDay);
        int year = cale.get(Calendar.YEAR);
        int month = cale.get(Calendar.MONTH) + 1;
        System.out.println("year：" + year);
        System.out.println("month：" + month);
*/

        boolean monthIfEquals = DateUtil.monthIfEquals(startTime, endTime); // 比如两个日期月份是否相等
        System.out.println("monthIfEquels：" + monthIfEquals);

        int differMonth = DateUtil.compareDate(startTime, endTime, 1); // 比较两个时间两差多少个月
        System.out.println("differMonth：" + differMonth);

        for(int i = 1; i < differMonth; i++){


            System.out.println("------i："+i);
//            System.out.println("aMonthFirstDayTime："+aMonthFirstDayTime);
            System.out.println("startTime："+startTime);

            String plusStartDate = DateUtil.plusMonth(startTime, i); // 下个月1号
            String plusEndDate = DateUtil.getAMonthLastDay(DateUtil.parseDate(plusStartDate, "yyyy-MM-dd").getTime()); // 下个月月底

            System.out.println("plusStartDate："+plusStartDate);
            System.out.println("plusEndDate："+plusEndDate);

            // 判断两个日期的月份是否相等

        }


        System.out.println("当月天数："+DateUtil.getMonthLastDay(aMonthLastDay));

        Date month2 = DateUtil.parseDate("2020-09", "yyyy-MM");
        System.out.println("month2："+month2.getTime());


    }



}
