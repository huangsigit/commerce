/**
 * DateUtil工具类API速查表:
 * 1.得到当前时间 getCurrentDate()
 * 2.得到当前年份字符串 getCurrentYear()
 * 3.得到当前月份字符串 getCurrentMonth()
 * 4.得到当天字符串 getCurrentDay()
 * 5.得到当前星期字符串(星期几) getCurrentWeek()
 * 6.Date转化为String formatDate()
 * 7.String转化为Date parseDate()
 * 8.比较时间大小 compareToDate()
 * 9.得到给定时间的给定天数后的日期 getAppointDate()
 * 10.获取两个日期之间的天数 getDistanceOfTwoDate()
 * 11.获取过去的天数 pastDays()
 * 12.获取过去的小时 pastHour()
 * 13.获取过去的分钟  pastMinutes()
 * 14.得到本周的第一天  getFirstDayOfWeek()
 * 15.得到当月第一天 getFirstDayOfMonth()
 * 16.得到下月的第一天 getFirstDayOfNextMonth()
 * 17.根据生日获取年龄 getAgeByBirthDate()
 */
package com.egao.common.core.utils;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 日期时间工具类
 * Created by wangfan on 2017-6-10 上午10:10
 */
public class DateUtil {
    /**
     * 得到当前时间(yyyy-MM-dd HH:mm:ss)
     *
     * @return
     */
    public static String getCurrentDate() {
        return formatDate(new Date());
    }

    /**
     * 得到当前时间
     *
     * @param formate 格式
     * @return
     */
    public static String getCurrentDate(String formate) {
        return formatDate(new Date(), formate);
    }

    /**
     * 得到当前年份字符串
     */
    public static String getCurrentYear() {
        return formatDate(new Date(), "yyyy");
    }

    /**
     * 得到当前月份字符串
     */
    public static String getCurrentMonth() {
        return formatDate(new Date(), "MM");
    }

    /**
     * 得到当天字符串
     */
    public static String getCurrentDay() {
        return formatDate(new Date(), "dd");
    }

    /**
     * 得到当前星期字符串(星期几)
     */
    public static String getCurrentWeek() {
        return getCurrentWeek(new Date());
    }

    public static String getCurrentWeek(Date date) {
        return formatDate(date, "E");
    }

    /**
     * Date转化为String
     *
     * @param date
     * @param formate 格式
     * @return
     */
    public static String formatDate(Date date, String formate) {
        SimpleDateFormat sdf = new SimpleDateFormat(formate);
        sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return sdf.format(date);
    }

    /**
     * Date转化为String(yyyy-MM-dd HH:mm:ss)
     *
     * @param date
     * @return
     */
    public static String formatDate(Date date) {
        return formatDate(date, "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * String转化为Date
     *
     * @param date
     * @param formate
     * @return
     */
    public static Date parseDate(String date, String formate) {
        SimpleDateFormat sdf = new SimpleDateFormat(formate);
        try {
            return sdf.parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * String转化为Date
     *
     * @param date
     * @return
     */
    public static Date parseDate(String date) {

        return parseDate(date, "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * String转化为Date
     *
     * @param date
     * @return
     */
    public static Date parseDateStr(String date, String formate) {
        return parseDate(date, formate);
    }

    /**
     * 比较时间大小
     *
     * @param first
     * @param second
     * @return 返回0 first等于second, 返回-1 first小于second,, 返回1 first大于second
     */
    public static int compareToDate(String first, String second, String pattern) {
        DateFormat df = new SimpleDateFormat(pattern);
        Calendar cal1 = Calendar.getInstance();
        Calendar cal2 = Calendar.getInstance();
        try {
            cal1.setTime(df.parse(first));
            cal2.setTime(df.parse(second));
        } catch (ParseException e) {
            e.printStackTrace();
            System.out.println("比较时间错误");
        }
        int result = cal1.compareTo(cal2);
        if (result < 0) {
            return -1;
        } else if (result > 0) {
            return 1;
        }
        return 0;
    }

    /**
     * 比较时间大小
     *
     * @param first
     * @param second
     * @return 返回0 first等于second, 返回-1 first小于second,, 返回1 first大于second
     */
    public static int compareToDate(Date first, Date second) {
        int result = first.compareTo(second);
        if (result < 0) {
            return -1;
        } else if (result > 0) {
            return 1;
        }
        return 0;
    }

    /**
     * 得到给定时间的给定天数后的日期
     *
     * @return
     */
    public static Date getAppointDate(Date date, int day) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_YEAR, day);
        return calendar.getTime();
    }

    /**
     * 获取两个日期之间的天数
     *
     * @param before
     * @param after
     * @return
     */
    public static double getDistanceOfTwoDate(Date before, Date after) {
        long beforeTime = before.getTime();
        long afterTime = after.getTime();
        return (afterTime - beforeTime) / (1000 * 60 * 60 * 24);
    }

    /**
     * 获取两个日期之间的天数
     *
     * @param beforeStr
     * @param afterStr
     * @return
     */
    public static int getDistanceOfTwoDate(String beforeStr, String afterStr) {
        Date before = parseDate(beforeStr, "yyyy-MM-dd");
        Date after = parseDate(afterStr, "yyyy-MM-dd");
        long beforeTime = before.getTime();
        long afterTime = after.getTime();
        return (int)((afterTime - beforeTime + 86400000) / (1000 * 60 * 60 * 24));
    }


    /**
     * 获取过去的天数
     *
     * @param date
     * @return
     */
    public static long pastDays(Date date) {
        long t = new Date().getTime() - date.getTime();
        return t / (24 * 60 * 60 * 1000);
    }

    /**
     * 获取过去的小时
     *
     * @param date
     * @return
     */
    public static long pastHour(Date date) {
        long t = new Date().getTime() - date.getTime();
        return t / (60 * 60 * 1000);
    }

    /**
     * 获取过去的分钟
     *
     * @param date
     * @return
     */
    public static long pastMinutes(Date date) {
        long t = new Date().getTime() - date.getTime();
        return t / (60 * 1000);
    }

    /**
     * 得到本周的第一天
     *
     * @return
     */
    public static Date getFirstDayOfWeek() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        return cal.getTime();
    }

    /**
     * 得到当月第一天
     *
     * @return
     */
    public static Date getFirstDayOfMonth() {
        Calendar cal = Calendar.getInstance();
        int firstDay = cal.getMinimum(Calendar.DAY_OF_MONTH);
        cal.set(Calendar.DAY_OF_MONTH, firstDay);
        return cal.getTime();
    }

    /**
     * 得到下月的第一天
     *
     * @return
     */
    public static Date getFirstDayOfNextMonth() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, +1);
        int firstDay = cal.getMinimum(Calendar.DAY_OF_MONTH);
        cal.set(Calendar.DAY_OF_MONTH, firstDay);
        return cal.getTime();
    }

    /**
     * 根据生日获取年龄
     *
     * @param birtnDay
     * @return
     */
    public static int getAgeByBirthDate(Date birtnDay) {
        Calendar cal = Calendar.getInstance();
        if (cal.before(birtnDay)) {
            return 0;
        }
        int yearNow = cal.get(Calendar.YEAR);
        int monthNow = cal.get(Calendar.MONTH);
        int dayOfMonthNow = cal.get(Calendar.DAY_OF_MONTH);
        cal.setTime(birtnDay);
        int yearBirth = cal.get(Calendar.YEAR);
        int monthBirth = cal.get(Calendar.MONTH);
        int dayOfMonthBirth = cal.get(Calendar.DAY_OF_MONTH);
        int age = yearNow - yearBirth;
        if (monthNow <= monthBirth) {
            if (monthNow == monthBirth) {
                if (dayOfMonthNow < dayOfMonthBirth) {
                    age--;
                }
            } else {
                age--;
            }
        }
        return age;
    }

    /**
     * 得到给定日期所在的一周的日期
     *
     * @return
     */
    public static List<String> getWeekDays(Date date) {
        List<String> weekDays = new ArrayList<>();
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        // 如果是周日要减一天
        if (1 == cal.get(Calendar.DAY_OF_WEEK)) {
            cal.add(Calendar.DAY_OF_MONTH, -1);
        }
        cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
        weekDays.add(formatDate(cal.getTime(), "yyyy-MM-dd"));
        for (int i = 1; i < 5; i++) {
            cal.add(Calendar.DATE, 1);
            weekDays.add(formatDate(cal.getTime(), "yyyy-MM-dd"));
        }
        return weekDays;
    }

    /**
     * 得到给定日期所在的一月的日期
     *
     * @return
     */
    public static List<String> getMonthDays(Date date) {
        List<String> monthDays = new ArrayList<>();
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        int firstDay = cal.getMinimum(Calendar.DAY_OF_MONTH);
        cal.set(Calendar.DAY_OF_MONTH, firstDay);
        int cMonth = cal.get(Calendar.MONTH);
        monthDays.add(formatDate(cal.getTime(), "yyyy-MM-dd"));
        for (int i = 1; i < 31; i++) {
            cal.add(Calendar.DATE, 1);
            if (cMonth == cal.get(Calendar.MONTH)) {
                monthDays.add(formatDate(cal.getTime(), "yyyy-MM-dd"));
            }
        }
        return monthDays;
    }

    /**
     * 时间戳格式化成日期
     */
    public static String timestampToTime(long time){
        try {
            if(time<=0){
                return "-";
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
            String formatTime = sdf.format(time);
            return formatTime;
        } catch (Exception e) {
            e.printStackTrace();
            String formatTime = timestampToTime(System.currentTimeMillis());
            return formatTime;
        }
    }

    /**
     * 时间戳格式化成日期
     */
    public static String timestampToTime(long time, String pattern){
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(pattern);
            sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
            String formatTime = sdf.format(time);
            return formatTime;
        } catch (Exception e) {
            e.printStackTrace();
            String formatTime = timestampToTime(System.currentTimeMillis(), pattern);
            return formatTime;
        }
    }

    /**
     * 获取一个月第一天
     */
    public static String getAMonthFirstDay(long time){
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai")); // 设置时区
            Calendar cale = Calendar.getInstance();
            cale.setTimeInMillis(time);
            cale.set(Calendar.DAY_OF_MONTH, 1);
            String firstDay = sdf.format(cale.getTime());
            return firstDay;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 获取一个月最后一天
     */
    public static String getAMonthLastDay(long time){
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            sdf.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai")); // 设置时区
            Calendar c = Calendar.getInstance();
            c.setTimeInMillis(time);
            c.add(Calendar.MONTH, 1);
            c.set(Calendar.DAY_OF_MONTH, 0);
            String lastDay = sdf.format(c.getTime());
            return lastDay;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 判断两个日期的月份是否相等
     */
    public static boolean monthIfEquals(String startTime, String endTime){

        Date startDate = parseDateStr(startTime, "yyyy-MM-dd");
        Date endDate = parseDateStr(endTime, "yyyy-MM-dd");


        Calendar cale = Calendar.getInstance();

        cale.setTime(startDate);
        int startYear = cale.get(Calendar.YEAR);
        int startMonth = cale.get(Calendar.MONTH) + 1;

        cale.setTime(endDate);
        int endYear = cale.get(Calendar.YEAR);
        int endMonth = cale.get(Calendar.MONTH) + 1;


        String startStr = "" + startYear + startMonth;
        String endStr = "" + endYear + endMonth;

        if(startStr.equals(endStr)){
            return true;
        }


        System.out.println("startStr：" + startStr);
        System.out.println("endStr：" + endStr);
        return false;
    }

    /**
     * @param date1 需要比较的时间 不能为空(null),需要正确的日期格式
     * @param date2 被比较的时间  为空(null)则为当前时间
     * @param stype 返回值类型   0为多少天，1为多少个月，2为多少年
     * @return
     */
    public static int compareDate(String date1,String date2,int stype){
        int n = 0;

        String[] u = {"天","月","年"};
        String formatStyle = stype==1?"yyyy-MM":"yyyy-MM-dd";

        date2 = date2==null ? getCurrentDate():date2;

        DateFormat df = new SimpleDateFormat(formatStyle);
        Calendar c1 = Calendar.getInstance();
        Calendar c2 = Calendar.getInstance();
        try {
            c1.setTime(df.parse(date1));
            c2.setTime(df.parse(date2));
        } catch (Exception e3) {
            System.out.println("wrong occured");
        }
        //List list = new ArrayList();
        while (!c1.after(c2)) {                     // 循环对比，直到相等，n 就是所要的结果
            //list.add(df.format(c1.getTime()));    // 这里可以把间隔的日期存到数组中 打印出来
            n++;
            if(stype==1){
                c1.add(Calendar.MONTH, 1);          // 比较月份，月份+1
            }
            else{
                c1.add(Calendar.DATE, 1);           // 比较天数，日期+1
            }
        }
        System.out.println("n："+n);

        if(n == 0){
            while (!c2.after(c1)) {                     // 循环对比，直到相等，n 就是所要的结果
                //list.add(df.format(c1.getTime()));    // 这里可以把间隔的日期存到数组中 打印出来
                n++;
                if(stype==1){
                    c2.add(Calendar.MONTH, 1);          // 比较月份，月份+1
                }
                else{
                    c2.add(Calendar.DATE, 1);           // 比较天数，日期+1
                }
            }
        }

        n = n-1;

        if(stype==2){
            n = (int)n/365;
        }

        System.out.println(date1+" -- "+date2+" 相差多少"+u[stype]+":"+n);
        return n;
    }


    /**
     * 增加月份
     */
    public static String plusMonth(String dateStr, int plusMonth){

        Date date = parseDateStr(dateStr, "yyyy-MM-dd");

        Calendar cale = Calendar.getInstance();

        cale.setTime(date);
        int year = cale.get(Calendar.YEAR);
        int month = cale.get(Calendar.MONTH) + 1 + plusMonth;
        int day = cale.get(Calendar.DAY_OF_MONTH);

        String result = year + "-" + month + "-" + day;

        return result;
    }

    /**
     * 取得当月天数
     * */
    public static int getMonthLastDay(Date date)
    {
        Calendar a = Calendar.getInstance();
        a.setTime(date);
        a.set(Calendar.DATE, 1);//把日期设置为当月第一天
        a.roll(Calendar.DATE, -1);//日期回滚一天，也就是最后一天
        int maxDate = a.get(Calendar.DATE);
        return maxDate;
    }



}
