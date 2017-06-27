package com.pandatv.redis.sink.tools;

/**
 * Created by likaiqing on 2017/6/26.
 */
public class TimeHelper {
    private long curMil;
    private long timeout;

    public TimeHelper(long timeout) {
        this.timeout = timeout;
        curMil = System.currentTimeMillis();
    }

    public boolean checkout() {
        long cur = System.currentTimeMillis();
        if (cur > (curMil + timeout)) {
            curMil = cur;
            return true;
        }
        return false;
    }
}
