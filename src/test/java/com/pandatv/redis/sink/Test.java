package com.pandatv.redis.sink;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.stream.IntStream;

/**
 * Created by likaiqing on 2017/10/9.
 */
public class Test {
    @org.junit.Test
    public void test1(){
        IntStream.range(-3, 0).forEach(i->System.out.print(i));
    }
    @org.junit.Test
    public void test2(){
        DateTimeFormatter stf = DateTimeFormat.forPattern("yyyyMMddHHmm");
        long l = Long.parseLong(stf.print(stf.parseDateTime(String.valueOf("201710091611")).plusMinutes(-3)));
        System.out.println(l);
    }
}
