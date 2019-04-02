package com.wqj.spark.tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;


/**
 * @Auther: wqj
 * @Date: 2018/10/15 15:54
 * @Description: kafka消息堆积判断
 */
public class ShellMonitor {
    public static void main(String[] args) throws InterruptedException {

        //获取到需要检查的topic
        System.out.println("开始调用");
        Process ps = killProcess(new String[]{"/sh/kafka_monitor.sh", "com.wqj.spark.test.LogTest"});
        ps.waitFor();
        System.out.println("调用成功");


    }


    public static Process killProcess(String[] process_class) {
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(process_class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return process;

    }
}
