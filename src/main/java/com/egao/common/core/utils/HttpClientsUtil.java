package com.egao.common.core.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONObject;


/*
 * 利用HttpClient进行post请求的工具类
 */
public class HttpClientsUtil {


    private final static int MAX_CALL_NUM = 1;//最大调用次数
    private final static String CHARSET = "utf-8";

    private final static Logger logger = LoggerFactory.getLogger(HttpUtil.class);

    public static void close(HttpPost httpPost) {
        if(httpPost != null) {
            httpPost.abort();
            httpPost.releaseConnection();
        }
    }

    public static void close(HttpGet httpGet) {
        if(httpGet != null) {
            httpGet.abort();
            httpGet.releaseConnection();
        }
    }

    public static String call(String serverUrl, Map<String, Object> fromParameters) {
        String json =  JSONObject.toJSONString(fromParameters);
        return call(serverUrl, json, 1);


    }
    public static JSONObject callToJson(String serverUrl, Map<String, Object> fromParameters) {
        String json =  JSONObject.toJSONString(fromParameters);
        String retStr = call(serverUrl, json, 1);
        if(retStr == null || "".equals(retStr)) {
            return null;
        }

        return JSONObject.parseObject(retStr);
    }

    /**
     * 通过post方式进行http调用
     * @param serverUrl:接口地址
     * @param fromParameters:http参数
     * @param callNum:调用次数
     * @return
     */
    public static String call(String serverUrl, Map<String, Object> fromParameters, int callNum) {
        // 联网请求
        CloseableHttpClient httpClient = HttpClients.createDefault();
        // 根据默认超时限制初始化requestConfig
        RequestConfig requestConfig = RequestConfig.custom().setSocketTimeout(30000).setConnectTimeout(30000).build();

        HttpPost httpPost = new HttpPost(serverUrl);

        // 组装参数
        List<NameValuePair> parameters = new ArrayList<NameValuePair>();

        for (Map.Entry<String, Object> entry : fromParameters.entrySet()) {
            if (!"".equals(entry.getValue())) {
                parameters.add(new BasicNameValuePair(entry.getKey(), entry.getValue() + ""));
            }
        }
        String result = null;
        try {
            httpPost.setEntity(new UrlEncodedFormEntity(parameters, CHARSET));
            httpPost.setConfig(requestConfig);

            HttpResponse response = httpClient.execute(httpPost);
            HttpEntity entity = response.getEntity();
            if(entity != null) {
                result = EntityUtils.toString(entity, CHARSET);
            }
            logger.info("HttpUtil.call.result==>" + result);

            return result;
        } catch (Exception e) {
            logger.error("HttpUtil.call error:", e);

            if(callNum < MAX_CALL_NUM) { //出现异常继续调用
                return call(serverUrl, fromParameters, ++callNum);
            } else {
                return "E99999";
            }
        } finally {
            close(httpPost);
        }
    }

    public static String call(String serverUrl, String request, int callNum) {
        String responseStr = null;
        HttpPost httpPost = null;
        try {
            httpPost = new HttpPost(serverUrl);
            StringEntity se = new StringEntity(request, CHARSET);
            se.setContentEncoding(CHARSET);
            se.setContentType("application/json"); //服务端接收:@RequestBody Map<String, String> requestMap
            httpPost.setEntity(se);
            HttpResponse httpResponse = HttpClients.createDefault().execute(httpPost);
            HttpEntity entity = httpResponse.getEntity();
            if(entity != null) {
                responseStr = EntityUtils.toString(entity, CHARSET);
            }
            logger.info("HttpUtil.call.result==>" + responseStr);

            return responseStr;
        } catch (Exception e) {
            logger.error("HttpUtil.call error:", e);

            if(callNum < MAX_CALL_NUM) { //出现异常继续调用
                return call(serverUrl, request, ++callNum);
            } else {
                return "E99999";
            }
        } finally {
            close(httpPost);
        }
    }

    public static String callByForm(String url, String params) {
        HttpPost httpPost = null;
        try{
            httpPost = new HttpPost(url);

            //param参数，可以为param="key1=value1&key2=value2"的一串字符串,或者是jsonObject
//		    String param1="key1=value1&key2=value2";
            StringEntity stringEntity = new StringEntity(params);
            stringEntity.setContentType("application/x-www-form-urlencoded");
            httpPost.setEntity(stringEntity);

            @SuppressWarnings({ "resource", "deprecation" })
            HttpClient client = new DefaultHttpClient();
            HttpResponse httpResponse = client.execute(httpPost);
            String result = EntityUtils.toString(httpResponse.getEntity(), HTTP.UTF_8);
            return result;
        } catch(Exception e){
            return "E99999";
        } finally {
            close(httpPost);
        }

    }

    public static String callByGet(String url, String params) {
        if(params != null) {
            url = url + params;
        }
        HttpClient client = new DefaultHttpClient();
        HttpGet httpGet = null;
        try{
            httpGet = new HttpGet(url);
            HttpResponse httpResponse = client.execute(httpGet);
            String result = EntityUtils.toString(httpResponse.getEntity(), HTTP.UTF_8);
            return result;

            //微信那边采用的编码方式为ISO8859-1所以需要转化
//	        String json = new String(result.getBytes("ISO-8859-1"),"UTF-8");
//	        return json;
        } catch(Exception e){
            return "E99999";
        } finally {
            close(httpGet);
        }

    }

    /**
     * 获取真实ip
     * @param request
     * @return
     */
    public static String getIpAddress(HttpServletRequest request) {
        String ip = request.getHeader("x-forwarded-for");
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("HTTP_CLIENT_IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("HTTP_X_FORWARDED_FOR");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        return ip;
    }




}
