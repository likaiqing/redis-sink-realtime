package com.pandatv.redis.sink;

import com.google.common.collect.HashMultimap;

/**
 * Created by likaiqing on 2017/7/13.
 */
public class GuavaTest {
    public static void main(String[] args) {
        HashMultimap<String, String> hMap = HashMultimap.create();
        hMap.put("1","1");
    }
}
