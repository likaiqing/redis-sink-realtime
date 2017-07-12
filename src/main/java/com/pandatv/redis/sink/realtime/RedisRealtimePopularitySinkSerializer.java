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
public class RedisRealtimePopularitySinkSerializer implements RedisEventSerializer {
    private static final Logger logger = LoggerFactory.getLogger(RedisRealtimePopularitySinkSerializer.class);
    private static Set<String> minuteFields = new HashSet<>();
    private static Set<String> minuteRoomIdFields = new HashSet<>();
    //    private static Set<String> parDates = new HashSet<>();
    private static TimeHelper timeHelper = null;

    private static List<Event> events;
    private static Jedis jedis;

    private static boolean hset;
    private static int hsetExpire;
    private static String hsetKeyPrefix;
    private static String hsetKeyPreVar;
    private static String hsetKeyName;
    private static String hsetKeySuffix;
    private static String hsetField;
    private static String hsetValue;
    private static boolean hsetCascadHset;
    private static String hsetHashKeyPreVar;
    private static String hsetHashKeyName;
    private static boolean hsetClassificationCascad;

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
            if (hsetClassificationCascad) {
                hsetClassificationCascad(jedis);
            }
            if (hsetCascadHset && timeHelper.checkout()) {
                for (String field : minuteFields) {
                    executeCascadHset(field, jedis);
                }
                minuteFields.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
            err++;
        }
        return events.size() - err;
    }

    private void hsetClassificationCascad(Jedis jedis) {
        StringBuffer sb = new StringBuffer();
        int i=0;
        for (String field : minuteRoomIdFields) {
            if (++i==minuteRoomIdFields.size()-1){

            }
            sb.append(field).append(",");
        }

    }

    private void executeCascadHset(String field, Jedis jedis) {
        String parDate = field.substring(0, 8);
        String minuteKey = new StringBuffer(hsetKeyPrefix).append(field).append(RedisSinkConstant.redisKeySep).append(hsetKeyName).append(RedisSinkConstant.redisKeySep).append(hsetKeySuffix).toString();
        String newKey = new StringBuffer(hsetKeyPrefix).append(parDate).append(RedisSinkConstant.redisKeySep).append(hsetHashKeyName).append(RedisSinkConstant.redisKeySep).append(hsetKeySuffix).toString();
        int total = 0;
        for (String value : jedis.hvals(minuteKey)) {
            try {
                total = total + Integer.parseInt(value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        jedis.hset(newKey, field, total + "");
    }

    private void pipelineExecute(Event event, Pipeline pipelined) {
        Map<String, String> headers = event.getHeaders();
        String key = getKey(headers);
        String field = getField(headers);
        String value = getValue(headers);
        minuteFields.add(headers.get(hsetKeyPreVar.substring(2, hsetKeyPreVar.length() - 1)));
        minuteRoomIdFields.add(new StringBuffer(headers.get(hsetKeyPreVar.substring(2, hsetKeyPreVar.length() - 1))).append(RedisSinkConstant.redisKeySep).append(headers.get(hsetField.substring(2, hsetField.length() - 1))).toString());
//        parDates.add(headers.get("par_date"));
        pipelined.hset(key, field, value);
        pipelined.expire(key, hsetExpire);
    }

    private String getValue(Map<String, String> headers) {
        String value = hsetValue;
        if (hsetValue.contains("${")) {
            value = headers.get(hsetValue.substring(2, hsetValue.length() - 1));
        }
        return value;
    }

    private String getField(Map<String, String> headers) {
        String field = hsetField;
        if (hsetField.contains("${")) {
            field = headers.get(hsetField.substring(2, hsetField.length() - 1));
        }
        return field;
    }

    public String getKey(Map<String, String> headers) {
        Preconditions.checkArgument(hsetKeyPreVar.contains("${"), "hsetKeyPreVar must be variable");
        return new StringBuffer(hsetKeyPrefix).append(headers.get(hsetKeyPreVar.substring(2, hsetKeyPreVar.length() - 1))).append(RedisSinkConstant.redisKeySep).append(hsetKeyName).append(RedisSinkConstant.redisKeySep).append(hsetKeySuffix).toString();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Context context) {
        hset = context.getBoolean("hset", false);
        hsetExpire = context.getInteger("hsetExpire", 300);
        hsetKeyPrefix = context.getString("hsetKeyPrefix");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hsetKeyPrefix), "hsetKeyPrefix must not empty");
        if (StringUtils.isNotEmpty(hsetKeyPrefix) && !hsetKeyPrefix.endsWith(RedisSinkConstant.redisKeySep)) {
            hsetKeyPrefix = hsetKeyPrefix + RedisSinkConstant.redisKeySep;
        }
        hsetKeyPreVar = context.getString("hsetKeyPreVar");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hsetKeyPreVar), "hsetKeyPreVar must not empty");
        hsetKeyName = context.getString("hsetKeyName");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hsetKeyName), "hsetKeyName must not empty");
        hsetKeySuffix = context.getString("hsetKeySuffix");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hsetKeySuffix), "hsetKeySuffix must not empty");
        hsetField = context.getString("hsetField");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hsetField), "hsetField must not empty");
        hsetValue = context.getString("hsetValue");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hsetValue), "hsetValue must not empty");
        hsetCascadHset = context.getBoolean("hsetCascadHset", false);
        Long setCascadHsetTime = context.getLong("setCascadHsetTime", 45000l);
        timeHelper = new TimeHelper(setCascadHsetTime);
        hsetHashKeyName = context.getString("hsetHashKeyName");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hsetHashKeyName), "hsetHashKeyName must not empty");
        hsetHashKeyPreVar = context.getString("hsetHashKeyPreVar");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hsetHashKeyPreVar), "hsetHashKeyPreVar must not empty");
        hsetClassificationCascad = context.getBoolean("hsetClassificationCascad", false);
        logger.info("configure load conf sucess");
    }

    @Override
    public void configure(ComponentConfiguration conf) {

    }
}
