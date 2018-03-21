package com.pandatv.redis.sink.realtime;

import com.pandatv.redis.sink.constant.RedisSinkConstant;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Created by likaiqing on 2018/3/21.
 */
public class RedisRealtimeCpagentLogin implements RedisEventSerializer {

    private static List<Event> events;
    private static Jedis jedis;

    private static boolean sadd = false;
    private static boolean saddUseExpire = false;
    private static int saddExpire = 0;
    private static String saddKeyPrefix;
    private static String saddKeyPreVar;
    private static String saddKeyName;
    private static String saddKeySuffix;
    private static String saddValue;

    @Override
    public void initialize(List<Event> events, Jedis jedis) {
        this.events = events;
        this.jedis = jedis;
    }

    @Override
    public int actionList() {
        int suc = 0;
        Pipeline pipelined = jedis.pipelined();
        for (Event event : events) {
            try {
                pipelineExecute(event, pipelined);
                suc++;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        pipelined.sync();
        pipelined.clear();
        return suc;
    }

    private void pipelineExecute(Event event, Pipeline pipelined) {
        Map<String, String> headers = event.getHeaders();
        if (sadd) {
            executeSadd(headers, pipelined);
        }
    }

    private void executeSadd(Map<String, String> headers, Pipeline pipelined) {
        String[] saddKeyNameArr = saddKeyName.split("\\s+");
        String[] saddKeySuffixArr = saddKeySuffix.split("\\s+");
        String[] saddValueArr = saddValue.split("\\s+");
        String saddPreVar = headers.get(saddKeyPreVar.substring(2, saddKeyPreVar.length() - 1));
        for (int i = 0; i < saddKeyNameArr.length; i++) {
            String keyNameVar = saddKeyNameArr[i].trim();
            String keyName = getParamValue(headers, keyNameVar);
            if (StringUtils.isEmpty(keyName)) {
                continue;
            }
            String suffix = saddKeySuffixArr[i].trim();
            String value = getParamValue(headers, saddValueArr[i].trim());
            String saddKey = new StringBuffer(saddKeyPrefix).append(RedisSinkConstant.redisKeySep)
                    .append(saddPreVar).append(RedisSinkConstant.redisKeySep)
                    .append(keyName).append(RedisSinkConstant.redisKeySep)
                    .append(suffix).toString();
            pipelined.sadd(saddKey, value);
            if (saddUseExpire) {
                pipelined.expire(saddKey, saddExpire);
            }
        }
    }

    private String getParamValue(Map<String, String> headers, String keyName) {
        if (keyName.contains(RedisSinkConstant.redisKeySep)) {
            return Arrays.stream(keyName.split(RedisSinkConstant.redisKeySep)).map(name -> getParamValue(headers, name)).reduce((a, b) -> a + RedisSinkConstant.redisKeySep + b).get();
        } else if (keyName.contains("base64.encode")) {
            return Base64.getEncoder().encodeToString(headers.get(keyName.substring(keyName.indexOf("{") + 1, keyName.length() - 1)).getBytes());
        } else if (keyName.contains("${")) {
            String result = headers.get(keyName.substring(2, keyName.length() - 1));
            return result;
        }
        return keyName;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Context context) {
        sadd = context.getBoolean("sadd");
        saddExpire = context.getInteger("saddExpire");
        if (saddExpire == 0) {
            saddUseExpire = false;
        } else {
            saddUseExpire = true;
        }
        saddKeyPrefix = context.getString("saddKeyPrefix");
        saddKeyPreVar = context.getString("saddKeyPreVar");
        saddKeyName = context.getString("saddKeyName");
        saddKeySuffix = context.getString("saddKeySuffix");
        saddValue = context.getString("saddValue");


    }

    @Override
    public void configure(ComponentConfiguration conf) {

    }
}
