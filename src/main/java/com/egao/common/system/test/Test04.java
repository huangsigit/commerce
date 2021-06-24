package com.egao.common.system.test;

import com.egao.common.core.utils.DateUtil;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Test04 {

    public static void main(String[] args) {

/*
        String date = "2020-06";
        Date result = DateUtil.parseDate(date, "yyyy-MM");
        System.out.println("result："+result.getTime());
//        DateUtil.formatDate()
*/

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cale = Calendar.getInstance();
        cale.setTimeInMillis(1592205579000l);
        cale.set(Calendar.DAY_OF_MONTH, 1);//设置为1号,当前日期既为本月第一天
        String lastDay = format.format(cale.getTime());
        System.out.println("-----2------firstDay:"+lastDay);

        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(1592205579000l);
        c.add(Calendar.MONTH, 1);
        c.set(Calendar.DAY_OF_MONTH, 0);//设置为1号,当前日期既为本月第一天
        String first = format.format(c.getTime());
        System.out.println("=======3========last:"+first);

/*
        int i = 2;
        int j = 0;
        int ij = i/j;
        System.out.println("ij："+ij);
*/


        BigDecimal profit = new BigDecimal(737.3444);
        double value = profit.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        System.out.println("value："+value);

    }
}
