package com.pandatv.redis.sink.joda_test;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

/**
 * Created by likaiqing on 2017/7/14.
 */
public class JodaTest {
    @Test
    public void test1() {
        DateTimeFormatter stf = DateTimeFormat.forPattern("yyyyMMddHHmm");
        DateTime dateTime = stf.parseDateTime("201707141059");
        DateTime dateTime2 = stf.parseDateTime("201707141102");
        long l = Long.parseLong(stf.print(dateTime.plusMinutes(2)));
        System.out.println(l);
    }
}
