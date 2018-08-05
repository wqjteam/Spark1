package com.wqj;

/**
 * @Auther: wqj
 * @Date: 2018/7/26 10:59
 * @Description:
 */
public class JavaLru {
    public static void main(String[] args) {
        LruCache<String, String> cache = new LruCache<String, String>(5);
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
