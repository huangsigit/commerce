package com.egao.common.core.utils;

import net.sf.ehcache.CacheManager;
import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
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

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by ant on 2015/10/12.
 */
public class UHttpAgent {

    private int connectTimeout = 5000;  //5s
    private int socketTimeout = 5000;   //5s
    private int maxTotalConnections = 200;  //最大连接数，可调整
    
    private static UHttpAgent instance;

    private CloseableHttpClient httpClient;

    private UHttpAgent(){

    }

    public static UHttpAgent getInstance(){
        if(instance == null){
            instance = new UHttpAgent();

        }
        return instance;
    }

    public static UHttpAgent newInstance(){
        return new UHttpAgent();
    }

    public void get(String url, Map<String, String> params, UHttpFutureCallback callback){
    	String requestBody = getRequestBody(params);
    	String result = null;
    	if(requestBody!=null){

            try {
//                result = CacheManager.getInstance().loadHttpMessage(requestBody);
            } catch (Exception e) {
                e.printStackTrace();
            }

            if(result!=null){
    			callback.completed(result);
    		}else{
    			get(url, null, params, "UTF-8", callback);
    		}
    	}else{
    		get(url, null, params, "UTF-8", callback);
    	}
    }

    public void post(String url, Map<String, String> headers, Map<String, String> params, UHttpFutureCallback callback){
        List<NameValuePair> pairs = new ArrayList<NameValuePair>();
        if(params != null){

            for(String key : params.keySet()){
                pairs.add(new BasicNameValuePair(key, params.get(key)));
            }
        }

        post(url, headers, new UrlEncodedFormEntity(pairs, Charset.forName("UTF-8")), callback);
    }

    public void post(String url, Map<String, String> params, UHttpFutureCallback callback){

        List<NameValuePair> pairs = new ArrayList<NameValuePair>();
        if(params != null){

            for(String key : params.keySet()){
                pairs.add(new BasicNameValuePair(key, params.get(key)));
            }
        }

        post(url, null, new UrlEncodedFormEntity(pairs, Charset.forName("UTF-8")), callback);
    }

    /***
     * http get 请求 默认请求头 参数编码UTF-8
     * @param url       请求地址
     * @param params    请求参数
     * @return
     */
    public String get(String url, Map<String,String> params){

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
    			return get(url, null, params, "UTF-8");
    		}
    	}else{
    		return get(url, null, params, "UTF-8");
    	}
    }

    /***
     * http post 请求 默认请求头 参数编码UTF-8
     * @param url       请求地址
     * @param params    请求参数
     * @return
     */
    public String post(String url, Map<String, String> params){

        return post(url, null, params, "UTF-8");
    }

    /***
     * http get 异步请求方式 TODO: 目前实现还是同步，异步待实现
     * @param url
     * @param headers
     * @param params
     * @param encoding
     * @param callback
     */
    public void get(String url, Map<String, String> headers, Map<String,String> params, String encoding, UHttpFutureCallback callback){

        String result = get(url, headers, params, encoding);
        
        if(result != null){
        	callback.completed(result);
        	String requestBody = getRequestBody(params);
            try {
//                CacheManager.getInstance().addHttpMessage(requestBody, result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else{
            callback.failed(url + " failed");
        }

    }

    /***
     * http post 异步请求方式 TODO: 目前实现还是同步，异步待实现
     * @param url
     * @param headers
     * @param entity
     * @param callback
     */
    public void post4(String url, Map<String, String> headers, HttpEntity entity, UHttpFutureCallback callback){
    	String requestBody = getRequestBody(entity);
    	String result = null;
    	if(requestBody!=null){
    	    System.out.println("post---url："+url);
            System.out.println("post---requestBody："+requestBody);

            try {
//                result = CacheManager.getInstance().loadHttpMessage(url+requestBody);
            } catch (Exception e) {
//                e.printStackTrace();
            }
            System.out.println("post---result1:"+result);
    		if(result!=null){
    			callback.completed(result);
    		}else{
    			result = post(url, headers, entity);
                System.out.println("post---result2:"+result);
    			if(result != null){
                    try {
//                        CacheManager.getInstance().addHttpMessage(url+requestBody, result);
                    } catch (Exception e) {
//                        e.printStackTrace();
                    }
                    callback.completed(result);
                    System.out.println("post---completed1");
                }else{
                    callback.failed(url + "failed");
                    System.out.println("post---failed1");
                }
    		}
    	}else{
    		result = post(url, headers, entity);
            if(result != null){
                try {
//                    CacheManager.getInstance().addHttpMessage(url+requestBody, result);
                } catch (Exception e) {
//                    e.printStackTrace();
                }
                callback.completed(result);
                System.out.println("post---completed2");
            }else{
                callback.failed(url + "failed");
                System.out.println("post---failed2");
            }
    	}
    }

    public void post(String url, Map<String, String> headers, HttpEntity entity, UHttpFutureCallback callback){
        String requestBody = getRequestBody(entity);
        String result = null;
        if(requestBody!=null){
            System.out.println("post---url："+url);
            System.out.println("post---requestBody："+requestBody);

                result = post(url, headers, entity);
                System.out.println("post---result2:"+result);
                if(result != null){
//                    CacheManager.getInstance().addHttpMessage(url+requestBody, result);
                    callback.completed(result);
                    System.out.println("post---completed1");
                }else{
                    callback.failed(url + "failed");
                    System.out.println("post---failed1");
                }
        }else{
            result = post(url, headers, entity);
            if(result != null){
//                CacheManager.getInstance().addHttpMessage(url+requestBody, result);
                callback.completed(result);
                System.out.println("post---completed2");
            }else{
                callback.failed(url + "failed");
                System.out.println("post---failed2");
            }
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
    public String get(String url , Map<String, String> headers, Map<String,String> params, String encoding){

        if(this.httpClient == null){
            init();
        }

        String fullUrl = url;
        String urlParams = parseGetParams(params, encoding);

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
                HttpEntity entity = httpResponse.getEntity();
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

    /***
     * http post 请求
     * @param url       请求地址
     * @param headers   请求头
     * @param params    参数
     * @param encoding  编码 UTF-8等
     * @return
     */
    public String post(String url, Map<String,String> headers, Map<String,String> params, String encoding){

        List<NameValuePair> pairs = new ArrayList<NameValuePair>();
        if(params != null){

            for(String key : params.keySet()){
                pairs.add(new BasicNameValuePair(key, params.get(key)));
            }
        }

        return post(url, headers, new UrlEncodedFormEntity(pairs, Charset.forName(encoding)));
    }

    /***
     * http post 请求
     * @param url       请求地址
     * @param headers   请求头
     * @param entity    参数内容
     * @return
     */
    public String post(String url, Map<String,String> headers, HttpEntity entity){

        if(this.httpClient == null) {
            init();
        }

        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeaders(parseHeaders(headers));
        httpPost.setEntity(entity);

        ResponseHandler<String> responseHandler = new ResponseHandler<String>() {
            @Override
            public String handleResponse(HttpResponse httpResponse) throws IOException {
                HttpEntity entity = httpResponse.getEntity();

                return entity != null ? EntityUtils.toString(entity, Charset.forName("UTF-8")) : null;
            }
        };

        try {


            String body = httpClient.execute(httpPost, responseHandler);

            return body;

        } catch (IOException e) {
           // e.printStackTrace();
        }finally {
            httpPost.releaseConnection();
        }

        return null;

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

    public HttpClient getHttpClient(){

        return this.httpClient;
    }

    //销毁
    public void destroy(){

        if(this.httpClient != null){
            try{
                this.httpClient.close();
                this.httpClient = null;
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    public String getRequestBody(Map<String,String> params){
    	
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
    
    public String getRequestBody(HttpEntity entity){
    	
    	if(entity!=null){
	    	try{
	        	BufferedReader reader = new BufferedReader(new InputStreamReader(entity.getContent()));
	        	StringBuilder sb = new StringBuilder();
	        	String s = "";
    			while((s = reader.readLine())!=null){
    				sb.append(s);
    			}
	    		return sb.toString();
	    	}catch(Exception e){
	    		e.printStackTrace();
	    	}
    	}
    	return null;
    }
 public static void main(String[] args) {
	// String s= getInstance().get("http://192.168.0.172:8899/pay/apple/payCallback?transactionReceipt=%7B%0A%09%22signature%22%20%3D%20%22A6MXZ%2Foy7X5NdWlFrNVebawfKeWL5LXdQoF5n39RlixB5zdWkYy3pKUy%2F%2FSZ3RWZuntjzz1JchPDLC3KXIQ5%2Fo2pcBDsFpu%2BkEL6wOz9ka5U3ziwDvK4t5bLoqom9aqICcC7GgNwlTSAzlM3llCeR7bLvo1gONau%2Fet9w%2Ft64teF704goCsL4Bs08%2FQWNubr1q0eeOrEcwKn24ENXCsOTW%2BH8E1sMu0JYugw1Ku9YxXRy4q96fnGGpmvd2gzTuG4k0faTzfkZ%2FGIiEW%2FQjEiQzsZJuuGNq%2Funeo0CPEQUWCXLSUyWU%2Fz5PqF5b1PKCZSvLNdJnH9bVhjLt6lpxy4RhQAAAWAMIIFfDCCBGSgAwIBAgIIDutXh%2BeeCY0wDQYJKoZIhvcNAQEFBQAwgZYxCzAJBgNVBAYTAlVTMRMwEQYDVQQKDApBcHBsZSBJbmMuMSwwKgYDVQQLDCNBcHBsZSBXb3JsZHdpZGUgRGV2ZWxvcGVyIFJlbGF0aW9uczFEMEIGA1UEAww7QXBwbGUgV29ybGR3aWRlIERldmVsb3BlciBSZWxhdGlvbnMgQ2VydGlmaWNhdGlvbiBBdXRob3JpdHkwHhcNMTUxMTEzMDIxNTA5WhcNMjMwMjA3MjE0ODQ3WjCBiTE3MDUGA1UEAwwuTWFjIEFwcCBTdG9yZSBhbmQgaVR1bmVzIFN0b3JlIFJlY2VpcHQgU2lnbmluZzEsMCoGA1UECwwjQXBwbGUgV29ybGR3aWRlIERldmVsb3BlciBSZWxhdGlvbnMxEzARBgNVBAoMCkFwcGxlIEluYy4xCzAJBgNVBAYTAlVTMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEApc%2BB%2FSWigVvWh%2B0j2jMcjuIjwKXEJss9xp%2FsSg1Vhv%2BkAteXyjlUbX1%2FslQYncQsUnGOZHuCzom6SdYI5bSIcc8%2FW0YuxsQduAOpWKIEPiF41du30I4SjYNMWypoN5PC8r0exNKhDEpYUqsS4%2B3dH5gVkDUtwswSyo1IgfdYeFRr6IwxNh9KBgxHVPM3kLiykol9X6SFSuHAnOC6pLuCl2P0K5PB%2FT5vysH1PKmPUhrAJQp2Dt7%2Bmf7%2Fwmv1W16sc1FJCFaJzEOQzI6BAtCgl7ZcsaFpaYeQEGgmJjm4HRBzsApdxXPQ33Y72C3ZiB7j7AfP4o7Q0%2FomVYHv4gNJIwIDAQABo4IB1zCCAdMwPwYIKwYBBQUHAQEEMzAxMC8GCCsGAQUFBzABhiNodHRwOi8vb2NzcC5hcHBsZS5jb20vb2NzcDAzLXd3ZHIwNDAdBgNVHQ4EFgQUkaSc%2FMR2t5%2BgivRN9Y82Xe0rBIUwDAYDVR0TAQH%2FBAIwADAfBgNVHSMEGDAWgBSIJxcJqbYYYIvs67r2R1nFUlSjtzCCAR4GA1UdIASCARUwggERMIIBDQYKKoZIhvdjZAUGATCB%2FjCBwwYIKwYBBQUHAgIwgbYMgbNSZWxpYW5jZSBvbiB0aGlzIGNlcnRpZmljYXRlIGJ5IGFueSBwYXJ0eSBhc3N1bWVzIGFjY2VwdGFuY2Ugb2YgdGhlIHRoZW4gYXBwbGljYWJsZSBzdGFuZGFyZCB0ZXJtcyBhbmQgY29uZGl0aW9ucyBvZiB1c2UsIGNlcnRpZmljYXRlIHBvbGljeSBhbmQgY2VydGlmaWNhdGlvbiBwcmFjdGljZSBzdGF0ZW1lbnRzLjA2BggrBgEFBQcCARYqaHR0cDovL3d3dy5hcHBsZS5jb20vY2VydGlmaWNhdGVhdXRob3JpdHkvMA4GA1UdDwEB%2FwQEAwIHgDAQBgoqhkiG92NkBgsBBAIFADANBgkqhkiG9w0BAQUFAAOCAQEADaYb0y4941srB25ClmzT6IxDMIJf4FzRjb69D70a%2FCWS24yFw4BZ3%2BPi1y4FFKwN27a4%2Fvw1LnzLrRdrjn8f5He5sWeVtBNephmGdvhaIJXnY4wPc%2Fzo7cYfrpn4ZUhcoOAoOsAQNy25oAQ5H3O5yAX98t5%2FGioqbisB%2FKAgXNnrfSemM%2Fj1mOC%2BRNuxTGf8bgpPyeIGqNKX86eOa1GiWoR1ZdEWBGLjwV%2F1CKnPaNmSAMnBjLP4jQBkulhgwHyvj3XKablbKtYdaG6YQvVMpzcZm8w7HHoZQ%2FOjbb9IYAYMNpIr7N4YtRHaLSPQjvygaZwXG56AezlHRTBhL8cTqA%3D%3D%22%3B%0A%09%22purchase-info%22%20%3D%20%22ewoJIm9yaWdpbmFsLXB1cmNoYXNlLWRhdGUtcHN0IiA9ICIyMDE3LTEyLTA4IDAyOjA4OjMwIEFtZXJpY2EvTG9zX0FuZ2VsZXMiOwoJInVuaXF1ZS1pZGVudGlmaWVyIiA9ICIwNmE3NjllZTUxZWI4Y2RlOGFmNWZjYTdiNzQwMWY3MjRjNTM0MWQzIjsKCSJvcmlnaW5hbC10cmFuc2FjdGlvbi1pZCIgPSAiMTAwMDAwMDM1ODA3OTY2MCI7CgkiYnZycyIgPSAiMSI7CgkidHJhbnNhY3Rpb24taWQiID0gIjEwMDAwMDAzNTgwNzk2NjAiOwoJInF1YW50aXR5IiA9ICIxIjsKCSJvcmlnaW5hbC1wdXJjaGFzZS1kYXRlLW1zIiA9ICIxNTEyNzI3NzEwMjg4IjsKCSJ1bmlxdWUtdmVuZG9yLWlkZW50aWZpZXIiID0gIkVGQjEwRDRGLUVFRTAtNDE3My1CQjVELUE5MUY3RjA5MDVGRCI7CgkicHJvZHVjdC1pZCIgPSAiZWRvX2lvc18xIjsKCSJpdGVtLWlkIiA9ICIxMzAwNDY5MzQ2IjsKCSJiaWQiID0gImNvbS5lZG8uaW9zLnd1a29uZyI7CgkicHVyY2hhc2UtZGF0ZS1tcyIgPSAiMTUxMjcyNzcxMDI4OCI7CgkicHVyY2hhc2UtZGF0ZSIgPSAiMjAxNy0xMi0wOCAxMDowODozMCBFdGMvR01UIjsKCSJpcy10cmlhbC1wZXJpb2QiID0gImZhbHNlIjsKCSJwdXJjaGFzZS1kYXRlLXBzdCIgPSAiMjAxNy0xMi0wOCAwMjowODozMCBBbWVyaWNhL0xvc19BbmdlbGVzIjsKCSJvcmlnaW5hbC1wdXJjaGFzZS1kYXRlIiA9ICIyMDE3LTEyLTA4IDEwOjA4OjMwIEV0Yy9HTVQiOwp9%22%3B%0A%09%22environment%22%20%3D%20%22Sandbox%22%3B%0A%09%22pod%22%20%3D%20%22100%22%3B%0A%09%22signing-status%22%20%3D%20%220%22%3B%0A%7D&transactionIdentifier=1000000358079660&productId=edo_ios_1&orderID=1373916817734500357", null);
	 //String access_token = "169828343626819%7Cb70f91d41b806e3661ffba1a38196507";
	//	String input_token = "EAACadT4YPEMBAAZBvuC7ZCTI7YuPRdEU6Sv2cX1iXhJPZB7Rif8F3qckVTkzzNkHOiabIOCGGhRc8IVi94TKfvSSvRVVEKOYIq1aT38OHGuGXBkHnIa6iBrrVoDF5xTCY5CZBh7XZBDC9ca1T6ZAQrTNCk0BbfrIC3Fe4DLtcBIgZDZD";
	//	 String s= getInstance().get("https://graph.facebook.com/debug_token?"+ "access_token=" + access_token + "&input_token=" + input_token, null);
	//System.out.println(s);
//	String sss=URLDecoder.decode("%7B%22channelid%22%3A1701%2C%22appid%22%3A1%2C%22uu-data%22%3A%22eyJzZGtsb2dpbmlkIjo0MTU2LCJuYW1lIjoidGVzdDAwMSIsInVzZXJpZCI6NDE1NiwidG9rZW4iOiJGQ0YxNDBGNDlBNkQ3NDI3QUU5NEM4QkIxNkRDMjFCOThFQzlDMDJCNEJCMDdEOEIifQ%3D%3D%22%2C%22sdkloginid%22%3A%224156%22%7D");
	 
//	System.out.println(sss);
//	String url="http://api.edofun.net:8001/user/getToken?extension=%7B%22channelid%22%3A1701%2C%22appid%22%3A1%2C%22uu-data%22%3A%22eyJzZGtsb2dpbmlkIjo0MTU2LCJuYW1lIjoidGVzdDAwMSIsInVzZXJpZCI6NDE1NiwidG9rZW4iOiJGQ0YxNDBGNDlBNkQ3NDI3QUU5NEM4QkIxNkRDMjFCOThFQzlDMDJCNEJCMDdEOEIifQ%3D%3D%22%2C%22sdkloginid%22%3A%224156%22%7D&sign=c1a6f562f87a13d94d4909f793cca17e";
//	String s= getInstance().get(url,null);
//	System.out.println(s);
	 float PRICE_MICROS = 1000000.00F; // google play返回是美分万分之一的数值
	 float  i=4990000;
	 System.out.println(i/PRICE_MICROS);
	 System.out.println(i/PRICE_MICROS*100);
//	 System.out.println(StringUtils.decrypt("292E37066376BD5FF38EC6945980EAE3", YingZhengSDK.enc_ps_key));


      String result = getInstance().get("https://graph.facebook.com/v7.0/144436283227029/client_ad_accounts?access_token=EAAH92JtasVMBAJ2iHbMXEdLwzMZAH2PidkMGwvQbhFZCZAAcPmUHOxfwaPfNg4M3vXCBonOVZAHLIrj7gdZCJqT9pQs8CAMGrBp7ECuNKOdFIO5txnP3UylNAI959oXBqp1hZAJloEBqSvVdt3hVhXYDu7WGdoZCgZCqrqX0PVE5LKKdGtlzQMxZBmrY8YWjQARUZD&fields=id,name,account_id,spend", null);
      System.out.println("result："+result);


 }
}
