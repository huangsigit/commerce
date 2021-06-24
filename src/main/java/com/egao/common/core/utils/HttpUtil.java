package com.egao.common.core.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.http.*;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.springframework.http.*;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;

/**
 * http请求工具类
 *
 * @see HttpMethod 请求方式
 * @see MediaType 参数类型，表单（MediaType.APPLICATION_FORM_URLENCODED）、json（MediaType.APPLICATION_JSON）
 * Created by hs on 2020-12-14 上午 8:38.
 */
public class HttpUtil {

    private int connectTimeout = 5000;  //5s
    private int socketTimeout = 5000;   //5s
    private CloseableHttpClient httpClient;
    private int maxTotalConnections = 200;  //最大连接数，可调整

    private static HttpUtil httpUtil;

    public static HttpUtil getInstance(){
        if(httpUtil == null){
            httpUtil = new HttpUtil();

        }
        return httpUtil;
    }

    /**
     * get请求
     *
     * @param url
     * @param params 请求参数
     * @return
     */
    public static String get(String url, MultiValueMap<String, String> params) {
        return get(url, params, null);
    }

    /**
     * get请求
     *
     * @param url
     * @param params  请求参数
     * @param headers 请求头
     * @return
     */
    public static String get(String url, MultiValueMap<String, String> params, MultiValueMap<String, String> headers) {
        return request(url, params, headers, HttpMethod.GET);
    }

    /**
     * post请求
     *
     * @param url
     * @param params 请求参数
     * @return
     */
    public static String post(String url, MultiValueMap<String, String> params) {
        return post(url, params, null);
    }

    /**
     * post请求
     *
     * @param url
     * @param params  请求参数
     * @param headers 请求头
     * @return
     */
    public static String post(String url, MultiValueMap<String, String> params, MultiValueMap<String, String> headers) {
        return request(url, params, headers, HttpMethod.POST);
    }

    /**
     * 发送post请求，不带参数的
     * @param url 请求的地址
     * @return
     */
    public static String doPost(String url) throws URISyntaxException, ClientProtocolException, IOException {
        return post(url, null);
    }

    /**
     * 发送post请求，带参数的
     * @param url 请求的地址
     * @param params 请求的参数
     * @return
     */
    public static String doPost(String url, Map<String, Object> params) throws URISyntaxException, ClientProtocolException, IOException {
        //创建httpClient对象
        CloseableHttpClient httpClient = HttpClients.createDefault();
        // 创建httpPost连接
        HttpPost httpPost = new HttpPost(url);

        if(params != null){
            Set<Map.Entry<String,Object>> entrySet = params.entrySet();
            List<NameValuePair> nvps = new ArrayList<NameValuePair>();
            for (Map.Entry<String, Object> entry : entrySet) {
                if(entry.getValue()!=null){
                    nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue().toString()));
                }
            }
            //设置请求参数
            httpPost.setEntity(new UrlEncodedFormEntity(nvps,"utf-8"));
        }
        //构造请求的配置信息
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(1000)//创建连接的最长时间
                .setConnectionRequestTimeout(500)//从连接池获取连接的最长时间
                .setSocketTimeout(10*1000).build();//数据传输的最长时间
        // 设置配置信息
        httpPost.setConfig(requestConfig);

        CloseableHttpResponse response = null;
        try {
            // 利用httpClient执行httpGet请求
            response = httpClient.execute(httpPost);
            // 处理结果
//			if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
            // return EntityUtils.toString(response.getEntity(), "utf-8");
            String content =  EntityUtils.toString(response.getEntity(), "utf-8");
//				return new HttpResult(HttpStatus.SC_OK, content);
            return content;
//			}
        } finally {
            if (response != null) {
                response.close();
            }
        }
//		return null;
    }

    /**
     * put请求
     *
     * @param url
     * @param params 请求参数
     * @return
     */
    public static String put(String url, MultiValueMap<String, String> params) {
        return put(url, params, null);
    }

    /**
     * put请求
     *
     * @param url
     * @param params  请求参数
     * @param headers 请求头
     * @return
     */
    public static String put(String url, MultiValueMap<String, String> params, MultiValueMap<String, String> headers) {
        return request(url, params, headers, HttpMethod.PUT);
    }

    /**
     * delete请求
     *
     * @param url
     * @param params 请求参数
     * @return
     */
    public static String delete(String url, MultiValueMap<String, String> params) {
        return delete(url, params, null);
    }

    /**
     * delete请求
     *
     * @param url
     * @param params  请求参数
     * @param headers 请求头
     * @return
     */
    public static String delete(String url, MultiValueMap<String, String> params, MultiValueMap<String, String> headers) {
        return request(url, params, headers, HttpMethod.DELETE);
    }

    /**
     * 表单请求
     *
     * @param url
     * @param params  请求参数
     * @param headers 请求头
     * @param method  请求方式
     * @return
     */
    public static String request(String url, MultiValueMap<String, String> params, MultiValueMap<String, String> headers, HttpMethod method) {
        if (params == null) {
            params = new LinkedMultiValueMap<>();
        }
        return request(url, params, headers, method, MediaType.APPLICATION_FORM_URLENCODED);
    }

    /**
     * http请求
     *
     * @param url
     * @param params    请求参数
     * @param headers   请求头
     * @param method    请求方式
     * @param mediaType 参数类型
     * @return
     */
    public static String request(String url, Object params, MultiValueMap<String, String> headers, HttpMethod method, MediaType mediaType)  {
        if (url == null || url.trim().isEmpty()) {
            return null;
        }
        RestTemplate client = new RestTemplate();
        // header
        HttpHeaders httpHeaders = new HttpHeaders();
        if (headers != null) {
            httpHeaders.addAll(headers);
        }
        // 提交方式：表单、json
        httpHeaders.setContentType(mediaType);
        HttpEntity httpEntity = new HttpEntity(params, httpHeaders);
        System.out.println("httpEntity:"+httpEntity);

//        String content = EntityUtils.toString(httpEntity);


        ResponseEntity<String> response = client.exchange(url, method, httpEntity, String.class);
        return response.getBody();
    }

    public static String get(HttpGet httpGet, List<Map<String, Object>> headerList)  {
        String content = "";
        try {

            for(Map<String, Object> map : headerList){
                for(String key : map.keySet()){
                    String value = (String)map.get(key);
//                    System.out.println(key+"  "+value);
                    httpGet.addHeader(key, value);

                }
            }

            CloseableHttpClient httpClient = HttpClients.createDefault();
            CloseableHttpResponse response = null;

              try {
                // 利用httpClient执行httpGet请求
                response = httpClient.execute(httpGet);
                // 处理结果
                content = EntityUtils.toString(response.getEntity(), "utf-8");
                System.out.println("content:"+content.length());

            } finally {
                if (response != null) {
                    response.close();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        return content;
    }


    /***
     * http get 请求 默认请求头 参数编码UTF-8
     * @param url       请求地址
     * @param params    请求参数
     * @return
     */
    public String doGet(String url, Map<String,String> params){

        String requestBody = getRequestBody(params);
        String result = null;
        if(requestBody!=null){

            try {
//                result = CacheManager.getInstance().loadHttpMessage(requestBody);
            } catch (Exception e) {
                e.printStackTrace();
            }

            if(result!=null){
                return result;
            }else{
                return doGet(url, null, params, "UTF-8");
            }
        }else{
            return doGet(url, null, params, "UTF-8");
        }
    }

    /***
     * http get 请求
     * @param url       请求地址
     * @param headers   请求头
     * @param params    参数
     * @param encoding  编码 UTF-8等
     * @return
     */
    public String doGet(String url , Map<String, String> headers, Map<String,String> params, String encoding){

        if(this.httpClient == null){
            init();
        }

        String fullUrl = url;
        String urlParams = parseGetParams(params, encoding);
        // 处理特殊字符
        urlParams.replace("\"", "%22")
                .replace("{", "%7b").replace("}", "%7d");

        if (urlParams != null)
        {
            if (url.contains("?"))
            {
                fullUrl = url + "&" + urlParams;
            }
            else
            {
                fullUrl = url + "?" + urlParams;
            }
        }

        HttpGet getReq = new HttpGet(fullUrl.trim());
        getReq.setHeaders(parseHeaders(headers));
        ResponseHandler<String> responseHandler = new ResponseHandler<String>() {
            @Override
            public String handleResponse(HttpResponse httpResponse) throws IOException {
                org.apache.http.HttpEntity entity = httpResponse.getEntity();
                return entity != null ? EntityUtils.toString(entity) : null;
            }
        };

        try {

            String res = httpClient.execute(getReq, responseHandler);
            String requestBody = getRequestBody(params);
            try {
//                CacheManager.getInstance().addHttpMessage(requestBody, res);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return res;

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            getReq.releaseConnection();
        }

        return null;
    }

    public static String getRequestBody(Map<String,String> params){

        if(params!=null && params.size()>0){
            StringBuilder sb = new StringBuilder();
            for(String s :params.keySet()){
                sb.append(s).append(params.get(s));
            }
            return sb.toString();
        }else{
            return null;
        }

    }
    //解析请求头
    private Header[] parseHeaders(Map<String, String> headers){
        if(null == headers || headers.isEmpty()){
            return getDefaultHeaders();
        }

        Header[] hs = new BasicHeader[headers.size()];
        int i = 0;
        for(String key : headers.keySet()){
            hs[i] = new BasicHeader(key, headers.get(key));
            i++;
        }

        return hs;
    }
    //获取默认的请求头
    private Header[] getDefaultHeaders(){
        Header[] hs = new BasicHeader[2];
        hs[0] = new BasicHeader("Content-Type", "application/x-www-form-urlencoded");
        hs[1] = new BasicHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.146 Safari/537.36");
        return hs;
    }

    //将参数解析成get请求的参数格式
    private String parseGetParams(Map<String, String> data, String encoding){
        if(data == null || data.size() <= 0){
            return null;
        }

        StringBuilder result = new StringBuilder();

        Iterator<String> keyItor = data.keySet().iterator();
        while(keyItor.hasNext()){
            String key = keyItor.next();
            String val = data.get(key);

            try {
                result.append(key).append("=").append(URLEncoder.encode(val, encoding).replace("+", "%2B")).append("&");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        return result.deleteCharAt(result.length() - 1).toString();

    }

    //初始化， 创建httpclient实例
    private void init(){

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .setExpectContinueEnabled(true)
                .setAuthenticationEnabled(true)
                .build();

        HttpRequestRetryHandler retryHandler = new HttpRequestRetryHandler() {
            @Override
            public boolean retryRequest(IOException e, int retryNum, HttpContext httpContext) {

                if(retryNum >= 3){
                    return false;
                }


                if(e instanceof org.apache.http.NoHttpResponseException
                        || e instanceof org.apache.http.client.ClientProtocolException
                        || e instanceof java.net.SocketException){

                    return true;
                }

                return false;
            }
        };

        ConnectionKeepAliveStrategy myStrategy = new ConnectionKeepAliveStrategy() {
            @Override
            public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
                try{

                    HeaderElementIterator it = new BasicHeaderElementIterator
                            (response.headerIterator(HTTP.CONN_KEEP_ALIVE));
                    while (it.hasNext()) {
                        HeaderElement he = it.nextElement();
                        String param = he.getName();
                        String value = he.getValue();
                        if (value != null && param.equalsIgnoreCase
                                ("timeout")) {
                            return Long.parseLong(value) * 1000;
                        }
                    }
                    return 3 * 1000;

                }catch(Exception e){
                    e.printStackTrace();
                }

                return 0;
            }
        };


        try{

            SSLContext sslContext = new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
                //信任所有
                @Override
                public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                    return true;
                }
            }).build();

            HostnameVerifier hostnameVerifier = NoopHostnameVerifier.INSTANCE;
            SSLConnectionSocketFactory sslFactory = new SSLConnectionSocketFactory(sslContext, hostnameVerifier);

            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                    .register("http", PlainConnectionSocketFactory.getSocketFactory())
                    .register("https", sslFactory)
                    .build();

            PoolingHttpClientConnectionManager connMgr = new PoolingHttpClientConnectionManager( socketFactoryRegistry);
            connMgr.setMaxTotal(maxTotalConnections);
            connMgr.setDefaultMaxPerRoute((connMgr.getMaxTotal()));

            HttpClientBuilder builder = HttpClients.custom()
                    .setKeepAliveStrategy(myStrategy)
                    .setDefaultRequestConfig(requestConfig)
                    .setSslcontext(sslContext)
                    .setConnectionManager(connMgr)
                    .setRetryHandler(retryHandler);

            this.httpClient = builder.build();


        }catch (Exception e){
            e.printStackTrace();
        }

    }

/*
    public PlanEntity signupApiTask(String json) {
        String url = "http://app.zxqg.com:7879/api/kttx/signup";//测试
        String result = HttpClientUtil.doPostJson(url, json);
        String unescapeJsonStr = StringEscapeUtils.unescapeJson(result);
        unescapeJsonStr = unescapeJsonStr.substring(1, unescapeJsonStr.length()-1);

        JSONObject jsonObject = JSONObject.parseObject(unescapeJsonStr);
        PlanEntity planEntity = jsonObject.toJavaObject(PlanEntity.class);
        return planEntity;
    }
*/

}
