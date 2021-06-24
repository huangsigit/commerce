package com.egao.common.core.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.logging.Logger;

/**
 * JSON解析工具类
 * Created by hs on 2017-06-10 10:10
 */
public class SecurityUtil {

    static java.util.logging.Logger logger= Logger.getLogger(SecurityUtil.class.getName());

    public static String doEncrypt(Map<String, Object> map, String appkey) {
        StringBuilder originStr = new StringBuilder();

        Object[] keys = map.keySet().toArray();
        Arrays.sort(keys);

        for (Object key : keys) {
            Object value = map.get(key.toString());
            if (Objects.nonNull(value) && StringUtils.isNotBlank(value.toString())) {
                //sign不参与签名字符串的拼接
                if ("sign".equals(key.toString().trim())) {
                    continue;
                }
                if ("extInfo".equals(key)) {
                    value = parseExtInfo((Map) value);
                }
                originStr.append(key).append("=").append(value).append("&");
            }
        }


        originStr.append("key=").append(appkey);

        logger.info("签名信息，拼接后信息为：[" + originStr.toString() + "]");
        String sign = encryptSHA(originStr.toString());
        logger.info("获取到的签名信息为：[" + sign + "]");
        return sign;
    }

    public static String parseExtInfo(Map extInfo) {
        Object[] keys = extInfo.keySet().toArray();
        Arrays.sort(keys);
        StringBuilder extInfoValue = new StringBuilder();
        for (Object key : keys) {
            Object value = extInfo.get(key.toString());
            if (Objects.nonNull(value) && StringUtils.isNotBlank(value.toString())) {
                extInfoValue.append(key).append("=").append(value.toString()).append("&");
            }
        }
        return extInfoValue.substring(0, extInfoValue.length() - 1);
    }

    public static final String encryptSHA(String data) {
        try {
//            return org.apache.commons.codec.digest.DigestUtils.sha256Hex(data);
            byte[] bytes = SHACoder.encodeSHA256(data.getBytes("UTF-8"));
            return Hex.encodeHexString(bytes);
        } catch (Exception e) {
            throw new RuntimeException("jiami错误，错误信息：", e);
        }
    }

    public static void main(String[] args) throws Exception {

        Map<String, Object> map = new HashMap<>();
        map.put("amount", "10");
        map.put("appId", "0000000001");
        map.put("country", "CN");
        map.put("currency", "CNY");

        map.put("merTransNo", "1595986726000");
        map.put("notifyUrl", "https://ostudiogame.com/pay/getOrderID?sign=AAABBB");
        map.put("prodName", "southeast.asia");

//        map.put("returnUrl", "https://ostudiogame.com/index/index.htm");
        map.put("version", "1.1");

        Map<String, Object> extInfoMap = new HashMap<>();
        extInfoMap.put("paymentTypes", "credit,debit,ewallet,upi");
        map.put("extInfo", extInfoMap);





        String str = doEncrypt(map, "bc2d5fc0c8d2442d86c9f4fd2d4a0b6b");
        System.out.println("str："+str);

    }


}
