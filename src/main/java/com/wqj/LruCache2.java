package com.wqj;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @Auther: wqj
 * @Date: 2018/7/26 15:40
 * @Description:
 */
public class LruCache2<K, V> extends LinkedHashMap<K, V> {
    private int cacheSize;
    public LruCache2(int max) {
        super(max,0.75F,true);
        this.cacheSize=max;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {

        return this.size()>cacheSize;
    }

    public static void main(String[] args) {
        LruCache2<String, String> cache = new LruCache2<String, String>(5);
        cache.put("1", "1");
        cache.put("2", "2");
        cache.put("3", "3");
        cache.put("4", "4");
        cache.put("5", "5");

        System.out.println("初始：");
        System.out.println(cache);
        System.out.println("访问3：");
        cache.get("3");
        System.out.println(cache);
        System.out.println("访问2：");
        cache.get("2");
        System.out.println(cache);
        System.out.println("增加数据6,7：");
        cache.put("6", "6");
        cache.put("7", "7");
        System.out.println(cache);
    }
}
