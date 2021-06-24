package com.egao.common.api.controller;

import com.alibaba.fastjson.JSONObject;
import com.egao.common.core.annotation.OperLog;
import com.egao.common.core.utils.HttpClientUtil;
import com.egao.common.core.utils.SecurityUtil;
import com.egao.common.core.web.JsonResult;
import org.apache.tika.utils.DateUtils;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Controller
@RequestMapping("/api")
public class WintecPayController {

    // 测试环境
    public static final String URL = "http://shop.ling-chuang.com:18099/winapi/clientapi/unifiedorder";

    @OperLog(value = "WintecPay", desc = "WintecPay支付")
    @ResponseBody
    @RequestMapping(value = "/wintecPay", produces = "application/json;charset=UTF-8")
    public JsonResult wintecPay(HttpServletRequest request, @RequestParam(name = "amount", required = false)String amount
            , @RequestParam(name = "appId", required = false)String appId, @RequestParam(name = "country", required = false)String country
            , @RequestParam(name = "currency", required = false)String currency, @RequestParam(name = "merTransNo", required = false)String merTransNo
            , @RequestParam(name = "notifyUrl", required = false)String notifyUrl, @RequestParam(name = "prodName", required = false)String prodName
            , @RequestParam(name = "returnUrl", required = false)String returnUrl, @RequestParam(name = "version", required = false)String version
            , @RequestParam(name = "extInfo", required = false)String extInfo, @RequestParam(name = "appkey", required = false)String appkey) {

        try {

            System.out.println("WintecPay wintecPay start："+ DateUtils.formatDate(new Date()));

            Map<String, Object> map = new HashMap<>();
            map.put("amount", amount);
            map.put("appId", appId);
            map.put("country", country);
            map.put("currency", currency);

            map.put("merTransNo", merTransNo);
            map.put("notifyUrl", notifyUrl);
            map.put("prodName", prodName);

            map.put("returnUrl", returnUrl);
            map.put("version", version);

//            Map<String, Object> extInfoMap = new HashMap<>();
//            extInfoMap.put("paymentTypes", paymentTypes);
            map.put("extInfo", extInfo);

            System.out.println("map:"+map);

            String sign = SecurityUtil.doEncrypt(map, appkey);
            map.put("sign", sign);

            String paramStr = JSONObject.toJSONString(map);
            System.out.println("paramStr:"+paramStr);
            String result = HttpClientUtil.doPost(URL, paramStr, "utf-8");
            System.out.println("response data：："+result);

            JsonResult data = JsonResult.ok(result);

            return data;
        } catch (Exception e) {
            e.printStackTrace();
            return JsonResult.error("fail");
        }
    }



    @OperLog(value = "WintecPay", desc = "通知")
    @ResponseBody
    @RequestMapping(value = "/notify", produces = "application/json;charset=UTF-8")
    public JsonResult notify(HttpServletRequest request, @RequestParam(name = "keyword", required = false)String keyword) {

        try {

            System.out.println("WintecPay notify："+ DateUtils.formatDate(new Date()));


            JsonResult data = JsonResult.ok(200, "ok");


            return data;
        } catch (Exception e) {
            e.printStackTrace();
            return JsonResult.error("fail");
        }
    }


    @OperLog(value = "WintecPay", desc = "通知")
    @ResponseBody
    @RequestMapping(value = "/returnSync", produces = "application/json;charset=UTF-8")
    public JsonResult returnSync(HttpServletRequest request, @RequestParam(name = "keyword", required = false)String keyword) {

        try {

            System.out.println("WintecPay returnSync："+ DateUtils.formatDate(new Date()));


            JsonResult data = JsonResult.ok(200, "ok");


            return data;
        } catch (Exception e) {
            e.printStackTrace();
            return JsonResult.error("fail");
        }
    }



}
