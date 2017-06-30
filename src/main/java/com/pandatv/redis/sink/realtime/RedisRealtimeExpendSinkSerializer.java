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

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by likaiqing on 2017/6/23.
 */
public class RedisRealtimeExpendSinkSerializer implements RedisEventSerializer {
    private static final Logger logger = LoggerFactory.getLogger(RedisRealtimeExpendSinkSerializer.class);
    private static Set<String> minuteNameFields = new HashSet<>();
//    private static Set<String> parDates = new HashSet<>();

    private static List<Event> events;
    private static Jedis jedis;

    private static String condition = null;

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
            if (saddCascadHset) {
                for (String field : minuteNameFields) {
                    executeCascadHset(field, jedis);
                }
                minuteNameFields.clear();
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
        String minute = field.substring(0, field.indexOf(RedisSinkConstant.redisKeySep));
        String name = field.substring(field.indexOf(RedisSinkConstant.redisKeySep) + 1);
        String minuteNameKey = new StringBuffer(saddKeyPrefix).append(field).append(RedisSinkConstant.redisKeySep).append(saddKeySuffix).toString();
        String newKey = new StringBuffer(saddKeyPrefix).append(parDate).append(RedisSinkConstant.redisKeySep).append(name).append(RedisSinkConstant.redisKeySep).append(saddKeySuffix).toString();
        Long uv = jedis.scard(minuteNameKey);
        jedis.hset(newKey, minute, String.valueOf(uv));
    }


    private void pipelineExecute(Event event, Pipeline pipelined) {
        Map<String, String> headers = event.getHeaders();
        if (checkEvent(event)) {
            if (sadd) {
                executeSadd(headers, pipelined);
            }
            if (hincrby) {
                executeHincrby(headers, pipelined);
            }
        }
    }

    private boolean checkEvent(Event event) {
        Map<String, String> headers = event.getHeaders();
        if (!condition.equalsIgnoreCase("none")) {
            if (condition.contains("&&")) {
                String[] conArr = condition.split("&&");
                boolean flag = true;
                for (String con : conArr) {
                    boolean resFlag = checkCondition(headers, con);
                    flag = flag && resFlag;
                }
                return flag;
            } else if (condition.contains("||")) {
                String[] conArr = condition.split("&&");
                boolean flag = false;
                for (String con : conArr) {
                    boolean resFlag = checkCondition(headers, con);
                    flag = flag || resFlag;
                }
                return flag;
            } else {
                return checkCondition(headers, condition);
            }
        }
        return true;
    }

    private boolean checkCondition(Map<String, String> headers, String con) {
        try {
            if (con.contains("}==")) {//相等
                String[] split = con.split("==");
                return headers.get(split[0].substring(2, split[0].length() - 1)).equals(split[1]);
            } else if (con.contains("}!=")) {//不相等
                String[] split = con.split("!=");
                return !headers.get(split[0].substring(2, split[0].length() - 1)).equals(split[1]);
            } else if (con.contains("}=~")) {//包含
                String[] split = con.split("=~");
                return headers.get(split[0].substring(2, split[0].length() - 1)).contains(split[1]);
            } else if (con.contains("}!~")) {//不包含
                String[] split = con.split("!~");
                return !headers.get(split[0].substring(2, split[0].length() - 1)).contains(split[1]);
            } else if (con.contains("}~=(")) {
                String[] split = con.split("~=");
                String headValue = headers.get(split[0].substring(2, split[0].length() - 1));
                String regx = split[1].substring(1, split[1].length() - 1);
                Pattern compile = Pattern.compile(regx);
                Matcher matcher = compile.matcher(headValue);
                return matcher.find();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    private void executeSadd(Map<String, String> headers, Pipeline pipelined) {
        String[] saddKeyNameNameArr = saddKeyName.split("\\s+");
        String[] saddKeySuffixArr = saddKeySuffix.split("\\s+");
        String[] saddValueArr = saddValue.split("\\s+");
        Preconditions.checkArgument(saddKeyNameNameArr.length == saddKeySuffixArr.length && saddKeyNameNameArr.length == saddValueArr.length, "must saddKeyNameNameArr.length == saddKeySuffixArr.length && saddKeyNameNameArr.length == saddValueArr.length ");
        for (int i = 0; i < saddKeyNameNameArr.length; i++) {
            String name = getKeyName(headers, saddKeySuffixArr[i]);
            String suffix = saddKeySuffixArr[i];
            String value = getValue(headers, saddValueArr[i]);
            String key = getSaddKey(headers, name, suffix);
            minuteNameFields.add(saddKeyPreVar + RedisSinkConstant.redisKeySep + name);
            pipelined.sadd(key, value);
            pipelined.expire(key, saddExpire);
        }
    }

    private String getSaddKey(Map<String, String> headers, String name, String suffix) {
        return new StringBuffer(saddKeyPrefix).append(headers.get(saddKeyPreVar.substring(2, saddKeyPreVar.length() - 1))).append(RedisSinkConstant.redisKeySep).append(name).append(RedisSinkConstant.redisKeySep).append(suffix).toString();
    }

    private void executeHincrby(Map<String, String> headers, Pipeline pipelined) {
        String[] hincrbyKeyNameArr = hincrbyKeyName.split("\\s+");
        String[] hincrbyKeySuffixArr = hincrbyKeySuffix.split("\\s+");
        String[] hincrbyFieldArr = hincrbyField.split("\\s+");
        String[] hincrbyValueArr = hincrbyValue.split("\\s+");
        Preconditions.checkArgument(hincrbyKeyNameArr.length == hincrbyKeySuffixArr.length && hincrbyKeyNameArr.length == hincrbyFieldArr.length && hincrbyKeyNameArr.length == hincrbyValueArr.length, "must hincrbyKeyNameArr.length==hincrbyKeySuffixArr.length&&hincrbyKeyNameArr.length==hincrbyFieldArr.length&&hincrbyKeyNameArr.length==hincrbyValueArr.length ");
        for (int i = 0; i < hincrbyKeyNameArr.length; i++) {
            String name = getKeyName(headers, hincrbyKeyNameArr[i]);
            String suffix = hincrbyKeySuffixArr[i];
            String field = getField(headers, hincrbyFieldArr[i]);
            int value = 1;
            try {
                value = Integer.parseInt(getValue(headers, hincrbyValueArr[i]));
            } catch (Exception e) {
                e.printStackTrace();
            }
            String hincrKey = getHincrKey(headers, name, suffix);
            pipelined.hincrBy(hincrKey, field, value);
        }
    }

    private String getValue(Map<String, String> headers, String value) {
        if (value.contains("${")) {
            return headers.get(value.substring(2, value.length() - 1));
        }
        return value;
    }

    private String getField(Map<String, String> headers, String field) {
        if (field.contains("${")) {
            headers.get(field.substring(2, field.length() - 1));
        }
        return field;
    }

    private String getKeyName(Map<String, String> headers, String keyName) {
        if (keyName.contains("base64.encode")) {
            return Base64.getEncoder().encodeToString(headers.get(keyName.substring(keyName.indexOf("{") + 1, keyName.length() - 1)).getBytes());
        }
        return keyName;
    }

    private String getHincrKey(Map<String, String> headers, String name, String suffix) {
        return new StringBuffer(hincrbyKeyPrefix).append(headers.get(hincrbyKeyPreVar.substring(2, hincrbyKeyPreVar.length() - 1))).append(RedisSinkConstant.redisKeySep).append(name).append(RedisSinkConstant.redisKeySep).append(suffix).toString();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Context context) {
        condition = context.getString("condition", "none");

        hincrby = context.getBoolean("hincrby", false);
        hincrbyKeyPrefix = context.getString("hincrbyKeyPrefix");
        if (StringUtils.isNotEmpty(hincrbyKeyPrefix) && !hincrbyKeyPrefix.endsWith(RedisSinkConstant.redisKeySep))
            hincrbyKeyPrefix = hincrbyKeyPrefix + RedisSinkConstant.redisKeySep;
        hincrbyKeyPreVar = context.getString("hincrbyKeyPreVar");
        hincrbyKeyName = context.getString("hincrbyKeyName");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyKeyName), "hincrbyKeyName must ");
        hincrbyKeySuffix = context.getString("hincrbyKeySuffix");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyKeySuffix), "hincrbyKeySuffix must ");
        hincrbyField = context.getString("hincrbyField");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyField), "hincrbyField must ");
        hincrbyValue = context.getString("hincrbyValue");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyValue), "hincrbyValue must ");


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
