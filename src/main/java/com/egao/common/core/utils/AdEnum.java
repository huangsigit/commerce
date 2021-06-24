package com.egao.common.core.utils;

public enum AdEnum {
    revenue("revenue", "收入", 1)
    , cost("cost", "广告成本", 2)
//    , roas("roas", "ROAS", 3)
    , logisticCost("logisticCost", "物流成本", 3)
    , goodsCost("goodsCost", "商品成本", 4)
    , operateCost("operateCost", "运营成本", 5)
    , refund("refund", "退款", 6)
    , toolCost("toolCost", "工具成本", 7)
    , passCost("passCost", "通道成本", 8)
    , profit("profit", "利润", 9);

    // 成员变量
    private String keys;
    private String values;
    private int index;
    // 构造方法
    private AdEnum(String keys, String values) {
        this.keys = keys;
        this.values = values;
    }

    // 构造方法
    private AdEnum(String keys, String values, int index) {
        this.keys = keys;
        this.values = values;
        this.index = index;
    }

    // 普通方法
    public static String getValues(String keys) {
        for (AdEnum c : AdEnum.values()) {
            if (c.getKeys().equals(keys)) {
                return c.values;
            }
        }
        return null;
    }

    public static String getValues(int index) {
        for (AdEnum c : AdEnum.values()) {
            if (c.getIndex() == index) {
                return c.values;
            }
        }
        return null;
    }

    public static String getKeys(int index) {
        for (AdEnum c : AdEnum.values()) {
            if (c.getIndex() == index) {
                return c.keys;
            }
        }
        return null;
    }

    // get set 方法
    public String getKeys() {
        return keys;
    }
    public void setKeys(String keys) {
        this.keys = keys;
    }
    public String getValues() {
        return values;
    }
    public void setValues(String values) {
        this.values = values;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }
}
