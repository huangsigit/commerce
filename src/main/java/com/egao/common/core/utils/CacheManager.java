/*
package com.egao.common.core.utils;

import com.alibaba.fastjson.JSONObject;
import com.u8.server.data.UChannel;
import com.u8.server.data.UChannelMaster;
import com.u8.server.data.UGame;
import com.u8.server.log.Log;
import com.u8.server.utils.MyJedisPool;
import com.u8.server.utils.RedisUtils;
import org.codehaus.jackson.map.ObjectMapper;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

*/
/***
 * 将常用的数据进行缓存。包含game,master,channel等对象
 *//*

public class CacheManager {

	private static CacheManager instance;

	// private Map<Integer, UGame> games;
	// private Map<Integer, UChannelMaster> masters;
	// private Map<Integer, UChannel> channels;

	private static String key_games = "k_games";
	private static String key_channles = "k_channles";
	private static String key_channlemaster = "k_cm";

	private static String key_http = "k_http_";

	public CacheManager() {

	}

	public static CacheManager getInstance() {
		if (instance == null) {
			instance = new CacheManager();
		}
		return instance;
	}


    */
/**
     * 加载Http请求结果
     *
     * @param request
     * @return
     *//*

    public String loadHttpMessage(String request) {
        return get(key_http, request, String.class);
    }

    public static void hset(String mkey, String key, Object value) {
        ObjectMapper mapper = new ObjectMapper();
        MyJedisPool my = RedisUtils.getCachePool();
        Jedis jedis = my.getResource();
        try {
            String json = mapper.writeValueAsString(value);
            jedis.hset(mkey, key, json);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            my.returnResource(jedis);
        }
    }

    */
/**
     * 添加Http请求结果
     *
     * @param request
     * @param value
     *//*

    public void addHttpMessage(String request, String value) {
        // setAndExpire(key_http, request, value, 60*2);//请求消息缓存2分钟
        setAndExpire(key_http, request, value, 60 * 2);// 请求消息缓存2分钟
    }

    public static void setAndExpire(String mkey, String key, Object value, int expireTime) {

        MyJedisPool my = RedisUtils.getCachePool();
        ObjectMapper mapper = new ObjectMapper();
        Jedis jedis = my.getResource();

        // Log.e("addHttpMessage--key->"+key+";value:"+value);

        try {
            String json = mapper.writeValueAsString(value);
            jedis.set(mkey + key, json);
            jedis.expire(mkey + key, expireTime);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            my.returnResource(jedis);
        }

    }

}
*/
