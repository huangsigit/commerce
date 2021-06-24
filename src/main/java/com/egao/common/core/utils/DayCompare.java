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
public class DayCompare{
    private int year;
    private int month;
    private int day;

    public DayCompare(int year, int month, int day) {
        this.year = year;
        this.month = month;
        this.day = day;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }
}