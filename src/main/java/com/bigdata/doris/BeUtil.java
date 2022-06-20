package com.bigdata.doris;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author
 * @description：TODO
 * @date 2022/5/13 11:15
 */
public class BeUtil {

    private static AtomicInteger pos = new AtomicInteger(0);
    /**
     * 轮询
     *
     * @return
     */
    public static String getBeRoundRobin(Map<String, Integer> beMap) {

        Set<String> keySet = beMap.keySet();
        ArrayList<String> keyList = new ArrayList<String>();
        keyList.addAll(keySet);
        if (pos.get() >= keySet.size()){
            pos = new AtomicInteger(0);
        }
        String server = keyList.get(pos.getAndIncrement());
        return server;
    }

    /**
     * 随机
     */
    public static String getBeRadom(Map<String, Integer> beMap) {
        // 取得Ip地址List
        Set<String> keySet = beMap.keySet();
        ArrayList<String> keyList = new ArrayList<String>();
        keyList.addAll(keySet);
        Random random = new Random();
        int randomPos = random.nextInt(keyList.size());
        return keyList.get(randomPos);
    }

    /**
     * 加权轮询
     */
    public static String getBeWeightRoundRobin(Map<String, Integer> beMap) {
        // 取得Ip地址List
        Set<String> keySet = beMap.keySet();
        Iterator<String> iterator = keySet.iterator();

        List<String> serverList = new ArrayList<String>();
        while (iterator.hasNext()) {
            String server = iterator.next();
            int weight = beMap.get(server);
            for (int i = 0; i < weight; i++){
                serverList.add(server);
            }
        }
        if (pos.get() >= keySet.size()){
            pos = new AtomicInteger(0);
        }
        String server = serverList.get(pos.getAndIncrement());
        return server;
    }

    /**
     * 加权随机
     */
    public static String getBeWeightRadom(Map<String, Integer> beMap) {

        // 取得Ip地址List
        Set<String> keySet = beMap.keySet();
        Iterator<String> iterator = keySet.iterator();

        List<String> serverList = new ArrayList<String>();
        while (iterator.hasNext()) {
            String server = iterator.next();
            int weight = beMap.get(server);
            for (int i = 0; i < weight; i++){
                serverList.add(server);
            }
        }
        Random random = new Random();
        int randomPos = random.nextInt(serverList.size());
        return serverList.get(randomPos);
    }

}
