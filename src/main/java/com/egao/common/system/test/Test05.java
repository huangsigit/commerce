package com.egao.common.system.test;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Test05 {

    public static void main(String[] args) {



        BigDecimal operateCost = new BigDecimal(24096626.963199993941918);
        double operateCostDouble = operateCost.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        System.out.println("operateCostDouble2："+operateCost.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());


    }
}
