package com.egao.common.core.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Logger;

import com.alibaba.fastjson.JSONObject;
//import util.Base64;

import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
//import org.apache.log4j.Logger;

import org.apache.http.message.BasicHeader;



/*
 * 利用HttpClient进行post请求的工具类
 */
public class HttpClientUtil {

//    private static final Logger logger = Logger.getLogger(HttpClientUtil.class);
    static java.util.logging.Logger logger= Logger.getLogger(SecurityUtil.class.getName());

    static{
        try {
            System.setProperty("jsse.enableSNIExtension", "false");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 发送post请求
     * @param url 请求路径
     * @param param 请求json数据
     * @return
     */
    public static String doPost(String url, JSONObject param){
        HttpPost httpPost = null;
        String result = null;
        try{
//            HttpClient client =  new SSLClient();
            HttpClient client =  HttpClients.createDefault();
            httpPost = new HttpPost(url);
            if(param != null){
                StringEntity se = new StringEntity(param.toString(),"utf-8");
//                se.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE, "application/json; charset="));
                httpPost.setEntity(se); //post方法中，加入json数据
                httpPost.setHeader("Content-Type","application/json");
            }

            HttpResponse response = client.execute(httpPost);
            if(response != null){
                HttpEntity resEntity = response.getEntity();
                if(resEntity != null){
                    result = EntityUtils.toString(resEntity,"utf-8");
                }
            }

        }catch(Exception ex){
            ex.printStackTrace();
        }
        System.out.println("返回结果:\n"+result);
        return result;
    }

    /**
     * 发送post请求
     * @param url 请求路径
     * @param jsonparam 请求json数据字符串
     * @return
     */
    public static String taikangPost(String url,String jsonparam){
        HttpPost httpPost = null;
        String result = null;
        try{
//            HttpClient client =  new SSLClient();
            HttpClient client =  HttpClients.createDefault();
            httpPost = new HttpPost(url);
            logger.info("请求路径:\n"+url);
            logger.info("请求参数:\n"+jsonparam);
            if(jsonparam != null){
                byte[] gbks = jsonparam.getBytes("GBK");
                jsonparam = Base64.encodeGBK(gbks);
                StringEntity se = new StringEntity(jsonparam,"GBK");
                httpPost.setEntity(se); //post方法中，加入json数据
                httpPost.setHeader("Content-Type","application/text");
            }

            HttpResponse response = client.execute(httpPost);

            if(response != null){
                HttpEntity resEntity = response.getEntity();
                if(resEntity != null){
                    result = EntityUtils.toString(resEntity,"GBK");
                    logger.info("返回结果原始字符串:\n"+result);
                    result = Base64.decode(result,"GBK");
                }
            }

        }catch(Exception ex){
            ex.printStackTrace();
        }
        logger.info("返回结果:\n"+result);
        return result;
    }


    /**
     * 发送post请求
     * @param url 请求路径
     * @param param 请求参数数据
     * @return
     */
    @SuppressWarnings("unchecked")
    public static String doPostMap(String url,Map<String, String> map){
        HttpClient httpClient = null;
        HttpPost httpPost = null;
        String result = null;
        try{
//            httpClient = new SSLClient();
            httpClient = HttpClients.createDefault();
            httpPost = new HttpPost(url);
            //设置参数
            List<NameValuePair> list = new ArrayList<NameValuePair>();
            Iterator iterator = map.entrySet().iterator();
            while(iterator.hasNext()){
                Entry<String,String> elem = (Entry<String, String>) iterator.next();
                list.add(new BasicNameValuePair(elem.getKey(),elem.getValue()));
            }
            if(list.size() > 0){
                UrlEncodedFormEntity entity = new UrlEncodedFormEntity(list,"UTF-8");
                httpPost.setEntity(entity);
            }
            HttpResponse response = httpClient.execute(httpPost);
            if(response != null){
                HttpEntity resEntity = response.getEntity();
                if(resEntity != null){
                    result = EntityUtils.toString(resEntity,"UTF-8");
                }
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
        logger.info("返回结果"+result);
        return result;
    }

    /**
     * 发送get请求
     * @param url 请求路径
     * @param param 请求json数据
     * @return
     */
    public static String doGet(String url){
        String result = null;
        try{
            HttpClient client = HttpClients.createDefault();
            //用get方法发送http请求
            HttpGet get = new HttpGet(url);
            CloseableHttpResponse httpResponse = null;
            //发送get请求
            httpResponse = (CloseableHttpResponse) client.execute(get);
            try{
                //response实体
                HttpEntity entity = httpResponse.getEntity();
                if (null != entity){
                    result = EntityUtils.toString(entity,"utf-8");
                }
            }
            finally{
                httpResponse.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    @SuppressWarnings("resource")
    public static String doPost(String url,String jsonstr,String charset){
        HttpClient httpClient = null;
        HttpPost httpPost = null;
        String result = null;
        try{
            httpClient = HttpClients.createDefault();
            httpPost = new HttpPost(url);
            httpPost.addHeader("Content-Type", "application/json");
            StringEntity se = new StringEntity(jsonstr);
            se.setContentType("text/json");
            se.setContentEncoding(new BasicHeader("Content-Type", "application/json"));
            httpPost.setEntity(se);
            HttpResponse response = httpClient.execute(httpPost);
            if(response != null){
                HttpEntity resEntity = response.getEntity();
                if(resEntity != null){
                    result = EntityUtils.toString(resEntity,charset);
                }
            }
        }catch(Exception ex){
            ex.printStackTrace();
        }
        return result;
    }


    public static void main(String[] args) {
        JSONObject obj = new JSONObject();
        obj.put("a", 1);
//        obj.element("a", 1);
//        HttpClientUtil.doPost("http://180.168.131.15/cpf/tianan_cpf/access/car/queryCarModel.mvc",obj);
        HttpClientUtil.taikangPost("http://ecuat.tk.cn/tkcoop_zz/service/proposalEntrance/proposalCreateEntrance?sign=ehaitun&comboid=1007A00901&fromId=62667",obj.toString());
    }


}
