package com.pandatv.redis.sink.realtime;

import com.google.common.base.Preconditions;
import com.pandatv.redis.sink.constant.RedisSinkConstant;
import com.pandatv.redis.sink.tools.TimeHelper;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by likaiqing on 2017/6/23.
 */
public class RedisRealtimeBarrageSinkSerializer implements RedisEventSerializer {
    private static final Logger logger = LoggerFactory.getLogger(RedisRealtimeBarrageSinkSerializer.class);
    private static Set<String> minuteFields = new HashSet<>();
//    private static Set<String> parDates = new HashSet<>();

    private static List<Event> events;
    private static Jedis jedis;

    private static boolean hincrby = false;
    private static String hincrbyKeyPrefix;
    private static String hincrbyKeyPreVar;
    private static String hincrbyKeyName;
    private static String hincrbyKeySuffix;
    private static String hincrbyField;
    private static String hincrbyValue;

    private static boolean sadd = false;
    private static int saddExpire;
    private static String saddKeyPrefix;
    private static String saddKeyPreVar;
    private static String saddKeyName;
    private static String saddKeySuffix;
    private static String saddValue;
    private static boolean saddCascadHset = false;
    private static String saddHashKeyPreVar;
    private static String saddHashKeyName;
    private static TimeHelper timeHelper;


    @Override
    public void initialize(List<Event> events, Jedis jedis) {
        this.events = events;
        this.jedis = jedis;
    }

    @Override
    public int actionList() {
        int err = 0;
        try {
            Pipeline pipelined = jedis.pipelined();
            for (Event event : events) {
                pipelineExecute(event, pipelined);
            }
            pipelined.sync();
            pipelined.clear();
            if (saddCascadHset && timeHelper.checkout()) {
                for (String field : minuteFields) {
                    executeCascadHset(field, jedis);
                }
                minuteFields.clear();
//                parDates.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
            err++;
        }
        return events.size() - err;
    }

    private void executeCascadHset(String field, Jedis jedis) {
        String parDate = field.substring(0, 8);
        String minuteKey = new StringBuffer(saddKeyPrefix).append(field).append(RedisSinkConstant.redisKeySep).append(saddKeyName).append(RedisSinkConstant.redisKeySep).append(saddKeySuffix).toString();
        String newKey = new StringBuffer(saddKeyPrefix).append(parDate).append(RedisSinkConstant.redisKeySep).append(saddHashKeyName).append(RedisSinkConstant.redisKeySep).append(saddKeySuffix).toString();
        Long uv = jedis.scard(minuteKey);
        jedis.hset(newKey, field, String.valueOf(uv));
    }


    private void pipelineExecute(Event event, Pipeline pipelined) {
        Map<String, String> headers = event.getHeaders();
        if (sadd) {
            executeSadd(headers, pipelined);
        }
        if (hincrby) {
            executeHincrby(headers, pipelined);
        }
    }

    private void executeSadd(Map<String, String> headers, Pipeline pipelined) {
        String key = getSaddKey(headers);
        String uid = headers.get(saddValue.substring(2, saddValue.length() - 1));
        pipelined.sadd(key, uid);
        pipelined.expire(key, saddExpire);
    }

    private String getSaddKey(Map<String, String> headers) {
        String minute = headers.get(saddKeyPreVar.substring(2, saddKeyPreVar.length() - 1));
        String parDate = headers.get("par_date");
        minuteFields.add(minute);
//        parDates.add(parDate);
        return new StringBuffer(saddKeyPrefix).append(minute).append(RedisSinkConstant.redisKeySep).append(saddKeyName).append(RedisSinkConstant.redisKeySep).append(saddKeySuffix).toString();
    }

    private void executeHincrby(Map<String, String> headers, Pipeline pipelined) {
        String key = getHincrKey(headers);
        String field = headers.get(hincrbyField.substring(2, hincrbyField.length() - 1));
        int value = 1;
        try {
            value = Integer.parseInt(hincrbyValue);
        } catch (Exception e) {
            e.printStackTrace();
        }
        pipelined.hincrBy(key, field, value);
    }

    private String getHincrKey(Map<String, String> headers) {
        return new StringBuffer(hincrbyKeyPrefix).append(headers.get(hincrbyKeyPreVar.substring(2, hincrbyKeyPreVar.length() - 1))).append(RedisSinkConstant.redisKeySep).append(hincrbyKeyName).append(RedisSinkConstant.redisKeySep).append(hincrbyKeySuffix).toString();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Context context) {
        hincrby = context.getBoolean("hincrby", false);
        hincrbyKeyPrefix = context.getString("hincrbyKeyPrefix");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyKeyPrefix), "hincrbyKeyPrefix must not null");
        hincrbyKeyPrefix = hincrbyKeyPrefix.endsWith(RedisSinkConstant.redisKeySep) ? hincrbyKeyPrefix : hincrbyKeyPrefix + RedisSinkConstant.redisKeySep;
        hincrbyKeyPreVar = context.getString("hincrbyKeyPreVar");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyKeyPreVar) && hincrbyKeyPreVar.contains("${"), "hincrbyKeyPreVar must not null and contains ${");
        hincrbyKeyName = context.getString("hincrbyKeyName");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyKeyName), "hincrbyKeyName must not null");
        hincrbyKeySuffix = context.getString("hincrbyKeySuffix");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyKeySuffix), "hincrbyKeySuffix must not null");
        hincrbyField = context.getString("hincrbyField");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyField) && hincrbyField.contains("${"), "hincrbyField must not null");
        hincrbyValue = context.getString("hincrbyValue");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyValue), "hincrbyValue must not null");

        sadd = context.getBoolean("sadd");
        saddExpire = context.getInteger("saddExpire", 1800);
        saddKeyPrefix = context.getString("saddKeyPrefix");
        Preconditions.checkArgument(StringUtils.isNotEmpty(saddKeyPrefix), "saddKeyPrefix must not null");
        saddKeyPrefix = saddKeyPrefix.endsWith(RedisSinkConstant.redisKeySep) ? saddKeyPrefix : saddKeyPrefix + RedisSinkConstant.redisKeySep;
        saddKeyPreVar = context.getString("saddKeyPreVar");
        Preconditions.checkArgument(StringUtils.isNotEmpty(saddKeyPreVar) && saddKeyPreVar.contains("${"), "saddKeyPreVar must not null and contains ${");
        saddKeyName = context.getString("saddKeyName");
        Preconditions.checkArgument(StringUtils.isNotEmpty(saddKeyName), "saddKeyName must not null");
        saddKeySuffix = context.getString("saddKeySuffix");
        Preconditions.checkArgument(StringUtils.isNotEmpty(saddKeySuffix), "saddKeySuffix must not null");
        saddValue = context.getString("saddValue");
        Preconditions.checkArgument(StringUtils.isNotEmpty(saddValue) && saddValue.contains("${"), "saddValue must not null and contains ${");
        saddCascadHset = context.getBoolean("saddCascadHset", false);
        saddHashKeyPreVar = context.getString("saddHashKeyPreVar");
        saddHashKeyName = context.getString("saddHashKeyName", "minute");
        long saddCascadHsetTime = context.getLong("saddCascadHsetTime", 45000l);
        timeHelper = new TimeHelper(saddCascadHsetTime);
    }

    @Override
    public void configure(ComponentConfiguration conf) {

    }
}
