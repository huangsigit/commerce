package com.egao.common.system.test;

import com.egao.common.core.Cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: hs
 * @Date: 2020/5/9 16:40
 * @Description: 缓存工具类测试
 */
public class CacheTest {
    public static void main(String[] args) throws InterruptedException {
        String key = "id";
        //不设置过期时间
        System.out.println("***********不设置过期时间**********");

        List list = new ArrayList();
        Map map = new HashMap<>();
        map.put("revenue", 100);
        map.put("cost", 50);
        list.add(map);

        Cache.put(key, 123);
        Cache.put("data", list);

        System.out.println("key:" + key + ", value:" + Cache.get(key));
        System.out.println("key:" + "data" + ", value:" + Cache.get("data"));
        Cache.get("itemsList");
        System.out.println("key:" + "itemsList" + ", value:" + Cache.get("itemsList"));

//        System.out.println("key:" + key + ", value:" + Cache.remove(key));
//        System.out.println("key:" + key + ", value:" + Cache.get(key));


/*
        //设置过期时间
        System.out.println("\n***********设置过期时间**********");
        Cache.put(key, "123456", 1000);
        System.out.println("key:" + key + ", value:" + Cache.get(key));
        Thread.sleep(2000);
        System.out.println("key:" + key + ", value:" + Cache.get(key));
*/


        System.out.println("当前缓存容量：" + Cache.size());
    }
}

