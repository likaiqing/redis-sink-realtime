package com.pandatv.redis.sink.realtime;

import com.google.common.base.Preconditions;
import com.pandatv.redis.sink.constant.RedisSinkConstant;
import com.sun.org.apache.xerces.internal.impl.dv.util.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by likaiqing on 2017/6/21.
 */
public class RedisRealtimeSinkSerializer implements RedisEventSerializer {
    private static final Logger logger = LoggerFactory.getLogger(RedisRealtimeSinkSerializer.class);

    private static List<Event> events;
    private static Jedis jedis;
    //incr
    private static boolean incr = false;
    private static int incrExpire = 0;
    private static String incrKeyPrefix;
    private static String incrKeyPreVar;
    private static String incrKeyName;
    private static String incrKeySuffix;
    private static String incrCondition;
    private static String incrValue;
    //sadd
    private static boolean sadd = false;
    private static int saddExpire = 0;
    private static String saddKeyPrefix;
    private static String saddKeyPreVar;
    private static String saddKeyName;
    private static String saddKeySuffix;
    private static String saddCondition;
    private static String saddValue;
    private static boolean saddCascadHset;
    //hincrby
    private static boolean hincrby = false;
    private static int hincrbyExpire = 0;
    private static String hincrbyKeyPrefix;
    private static String hincrbyKeyPreVar;
    private static String hincrbyKeyName;
    private static String hincrbyKeySuffix;
    private static String hincrbyKeyCondition;
    private static String hincrbyField;
    private static String hincrbyValue;
    private static String saddHashKeyPreVar;
    private static String saddHashKeyPrefix;
    private static String saddHashKeyName;
    private static String saddHashKeySuffix;

    @Override
    public void configure(Context context) {
        //hincrby
        hincrby = context.getBoolean("hincrby", false);
        try {
            String hincrbyExpireStr = context.getString("hincrbyExpire");
            if (StringUtils.isNotEmpty(hincrbyExpireStr)) {
                hincrbyExpire = Integer.parseInt(hincrbyExpireStr);
            }
        } catch (Exception e) {
            e.printStackTrace();
            hincrbyExpire = 0;
        }
        hincrbyKeyPrefix = context.getString("hincrbyKeyPrefix");
        if (StringUtils.isNotEmpty(hincrbyKeyPrefix) && !hincrbyKeyPrefix.endsWith(RedisSinkConstant.redisKeySep))
            hincrbyKeyPrefix = hincrbyKeyPrefix + RedisSinkConstant.redisKeySep;
        hincrbyKeyPreVar = context.getString("hincrbyKeyPreVar");
        hincrbyKeyName = context.getString("hincrbyKeyName");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyKeyName), "hincrbyKeyName must ");
        hincrbyKeySuffix = context.getString("hincrbyKeySuffix");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyKeySuffix), "hincrbyKeySuffix must ");
        hincrbyKeyCondition = context.getString("hincrbyKeyCondition");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyKeyCondition), "hincrbyKeyCondition must ");
        hincrbyField = context.getString("hincrbyField");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyField), "hincrbyField must ");
        hincrbyValue = context.getString("hincrbyValue");
        Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyValue), "hincrbyValue must ");

        //incr
        incr = context.getBoolean("incr", false);
        try {
            String incrExpireStr = context.getString("incrExpire");
            if (StringUtils.isNotEmpty(incrExpireStr)) {
                incrExpire = Integer.parseInt(incrExpireStr);
            }
        } catch (Exception e) {
            e.printStackTrace();
            incrExpire = 0;
        }
        incrKeyPrefix = context.getString("incrKeyPrefix");
        if (StringUtils.isNotEmpty(incrKeyPrefix) && !incrKeyPrefix.endsWith(RedisSinkConstant.redisKeySep))
            incrKeyPrefix = incrKeyPrefix + RedisSinkConstant.redisKeySep;
        incrKeyPreVar = context.getString("incrKeyPreVar");
        incrKeyName = context.getString("incrKeyName");
        Preconditions.checkArgument(StringUtils.isNotEmpty(incrKeyName), "incrKeyName must ");
        incrKeySuffix = context.getString("incrKeySuffix");
        Preconditions.checkArgument(StringUtils.isNotEmpty(incrKeySuffix), "incrKeySuffix must ");
        incrCondition = context.getString("incrCondition");
        Preconditions.checkArgument(StringUtils.isNotEmpty(incrCondition), "incrCondition must ");
        incrValue = context.getString("incrValue");
        //sadd
        sadd = context.getBoolean("sadd", false);
        try {
            String saddExpireStr = context.getString("saddExpire");
            if (StringUtils.isNotEmpty(saddExpireStr)) {
                saddExpire = Integer.parseInt(saddExpireStr);
                if (saddExpire >= 3600 * 23) {
                    saddExpire = 3600 * 23;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            saddExpire = 0;
        }
        saddKeyPrefix = context.getString("saddKeyPrefix", "rt_expend");
        if (StringUtils.isNotEmpty(saddKeyPrefix) && !saddKeyPrefix.endsWith(RedisSinkConstant.redisKeySep))
            saddKeyPrefix = saddKeyPrefix + RedisSinkConstant.redisKeySep;
        saddKeyPreVar = context.getString("saddKeyPreVar");
        saddKeyName = context.getString("saddKeyName");
        saddKeySuffix = context.getString("saddKeySuffix");
        saddCondition = context.getString("saddCondition");
        saddValue = context.getString("saddValue");
        saddCascadHset = context.getBoolean("saddCascadHset", false);
        saddHashKeyPreVar = context.getString("saddHashKeyPreVar");
        saddHashKeyPrefix = context.getString("saddHashKeyPrefix");
        saddHashKeyName = context.getString("saddHashKeyName");
        saddHashKeySuffix = context.getString("saddHashKeySuffix");
    }

    private void pipelineExecute(Event event, Jedis pipelined) {
        Map<String, String> headers = event.getHeaders();
        if (hincrby) {
            pipelineExecuteHincrby(headers, pipelined);
        }
        //incr
        if (incr) {
            pipelineExecuteIncr(headers, pipelined);
        }
        if (sadd) {
            pipelineExecuteSadd(headers, pipelined);
        }
    }

    private void pipelineExecuteHincrby(Map<String, String> headers, Jedis pipelined) {
        String hincrbyKey = hincrbyKeyPrefix;
        if (StringUtils.isNotEmpty(hincrbyKeyPreVar) && hincrbyKeyPreVar.contains("${")) {
            String keyPreValue = headers.get(hincrbyKeyPreVar.substring(2, hincrbyKeyPreVar.length() - 1));
            hincrbyKey = hincrbyKey + keyPreValue + RedisSinkConstant.redisKeySep;
        }
        String[] hincrbyKeyNameArr = hincrbyKeyName.split("\\s+");
        String[] hincrbyKeySuffixArr = hincrbyKeySuffix.split("\\s+");
        String[] hincrbyKeyConditionArr = hincrbyKeyCondition.split("\\s+");
        for (int i = 0; i < hincrbyKeyNameArr.length; i++) {
            String conditionStr = hincrbyKeyConditionArr[i];
            boolean checkContinue = checkContinue(conditionStr, headers);
            if (checkContinue) {
                continue;
            }
            String key = getKey(headers, hincrbyKey, hincrbyKeyNameArr, hincrbyKeySuffixArr, i);
            String[] hincrbyFieldArr = hincrbyField.split("\\s+");
            String[] hincrbyValueArr = hincrbyValue.split("\\s+");
            String field = getField(hincrbyFieldArr[i], headers);
            String valueStr = hincrbyValueArr[i];
            int intValue = getIntValue(valueStr, headers);
            pipelined.hincrBy(key, field, intValue);
//            if (saddExpire > 0) {
//                pipelined.expire(key, hincrbyExpire);
//            }
        }
    }

    private double getDoubleValue(String valueStr, Map<String, String> headers) {
        double result = 1.0;
        if (valueStr.contains("${")) {
            String headerValStr = headers.get(valueStr.substring(2, valueStr.length() - 1));
            try {
                result = Double.parseDouble(headerValStr);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            try {
                result = Double.parseDouble(valueStr);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    private int getIntValue(String valueStr, Map<String, String> headers) {
        int result = 1;
        if (valueStr.contains("${")) {
            String headerValStr = headers.get(valueStr.substring(2, valueStr.length() - 1));
            try {
                result = Integer.parseInt(headerValStr);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            try {
                result = Integer.parseInt(valueStr);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    private boolean isIntValue(String valueStr, Map<String, String> headers) {
        boolean intIncr = true;
        if (valueStr.contains("${")) {
            String headerValStr = headers.get(valueStr.substring(2, valueStr.length() - 1));
            if (StringUtils.isEmpty(headerValStr)) {
                intIncr = true;
            } else if (headerValStr.contains(".")) {
                intIncr = false;
            } else {
                intIncr = true;
            }
        } else if (valueStr.contains(".")) {
            intIncr = false;
        } else {
            intIncr = true;
        }
        return intIncr;
    }

    private String getField(String hincrbyField, Map<String, String> headers) {
        String result = hincrbyField;
        if (hincrbyField.contains("${")) {
            result = headers.get(hincrbyField.substring(2, hincrbyField.length() - 1));
        }
        return result;
    }

    private void pipelineExecuteSadd(Map<String, String> headers, Jedis pipelined) {
        String saddKey = saddKeyPrefix;
        if (StringUtils.isNotEmpty(saddKeyPreVar) && saddKeyPreVar.contains("${")) {
            String keyPreValue = headers.get(saddKeyPreVar.substring(2, saddKeyPreVar.length() - 1));
            saddKey = saddKey + keyPreValue + RedisSinkConstant.redisKeySep;
        }
        String[] saddKeyNameArr = saddKeyName.split("\\s+");
        String[] saddKeySuffixArr = saddKeySuffix.split("\\s+");
        String[] saddConditionArr = saddCondition.split("\\s+");
        String[] saddValueArr = saddValue.split("\\s+");
        Preconditions.checkArgument(saddKeyNameArr.length == saddKeySuffixArr.length && saddKeyNameArr.length == saddConditionArr.length && saddKeyNameArr.length == saddValueArr.length, "saddKeyName saddKeySuffix saddCondition saddValueArr size must equal");
        for (int i = 0; i < saddKeyNameArr.length; i++) {
            String conditionStr = saddConditionArr[i];
            boolean checkContinue = checkContinue(conditionStr, headers);
            if (checkContinue) {
                continue;
            }
            String key = getKey(headers, saddKey, saddKeyNameArr, saddKeySuffixArr, i);
            String valueStr = saddValueArr[i];
            Preconditions.checkArgument(valueStr.contains("${"), "saddvalue must ${}");
            String value = headers.get(valueStr.substring(2, valueStr.length() - 1));
            pipelined.sadd(key, value);
            if (saddExpire > 0) {
                pipelined.expire(key, saddExpire);
            }
            if (saddCascadHset) {
//                pipelined.sync();
//                try {
//                    pipelined.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
                long uv = pipelined.scard(key);
//                pipelined = jedis.pipelined();
                saddCascadHset(headers, key, uv, pipelined);
            }
        }
    }

    private void saddCascadHset(Map<String, String> headers, String key, long value, Jedis pipelined) {
        String saddKeyPreVarValue = headers.get(saddKeyPreVar.substring(2, saddKeyPreVar.length() - 1));
        String saddHashKeyPreVarValue = headers.get(saddHashKeyPreVar.substring(2, saddHashKeyPreVar.length() - 1));
        String hashKey = key.replace(saddKeyPreVarValue, saddHashKeyPreVarValue);
        pipelined.hset(hashKey, saddKeyPreVarValue, value + "");
    }

    private void pipelineExecuteIncr(Map<String, String> headers, Jedis pipelined) {
        String incrKey = incrKeyPrefix;
        if (StringUtils.isNotEmpty(incrKeyPreVar) && incrKeyPreVar.contains("${")) {
            String keyPreValue = headers.get(incrKeyPreVar.substring(2, incrKeyPreVar.length() - 1));
            incrKey = incrKey + keyPreValue + RedisSinkConstant.redisKeySep;
        }
        String[] incrKeyNameArr = incrKeyName.split("\\s+");
        String[] incrKeySuffixArr = incrKeySuffix.split("\\s+");
        String[] incrConditionArr = incrCondition.split("\\s+");
        String[] incrValueArr = incrValue.split("\\s+");
        Preconditions.checkArgument(incrKeyNameArr.length == incrKeySuffixArr.length && incrKeyNameArr.length == incrConditionArr.length && incrKeyNameArr.length == incrValueArr.length, "incrKeyName incrKeySuffix incrCondition incrValueArr size must equal");
        for (int i = 0; i < incrKeyNameArr.length; i++) {
            String conditionStr = incrConditionArr[i];
            boolean checkContinue = checkContinue(conditionStr, headers);
            if (checkContinue) {
                continue;
            }
            String key = getKey(headers, incrKey, incrKeyNameArr, incrKeySuffixArr, i);
            String valueStr = incrValueArr[i];
            int intValue = 1;
            double doubleValue = 1.0;
            boolean isIntValue = isIntValue(valueStr, headers);
            if (isIntValue) {
                intValue = getIntValue(valueStr, headers);
                pipelined.incrBy(key, intValue);
            } else {
                doubleValue = getDoubleValue(valueStr, headers);
                pipelined.incrByFloat(key, doubleValue);
            }
            if (incrExpire > 0) {
                pipelined.expire(key, incrExpire);
            }
        }
    }

    private String getKey(Map<String, String> headers, String incrKey, String[] incrKeyNameArr, String[] incrKeySuffixArr, int i) {
        String keyName = incrKeyNameArr[i].trim();
        if (keyName.contains("${")) {
            keyName = headers.get(keyName.substring(2, keyName.length() - 1));
            if (null == keyName) {
                keyName = "";
            }
        } else if (keyName.toLowerCase().contains("base64.encode")) {
            keyName = headers.get(keyName.substring(keyName.indexOf("{") + 1, keyName.length() - 1));
            if (null == keyName) {
                keyName = "";
            } else {
                keyName = Base64.encode(keyName.getBytes());
            }
        }
        String keySuf = incrKeySuffixArr[i];
        if (keySuf.equalsIgnoreCase("none")) {
            keySuf = "";
        } else {
            keySuf = RedisSinkConstant.redisKeySep + keySuf;
        }
        String key = incrKey + keyName + keySuf;
        return key;
    }

    private boolean checkContinue(String conditionStr, Map<String, String> headers) {
        if (!conditionStr.equalsIgnoreCase("none")) {
            if (conditionStr.contains("&&")) {
                String[] conArr = conditionStr.split("&&");
                boolean flag = true;
                for (String con : conArr) {
                    boolean resFlag = checkCondition(headers, con);
                    flag = flag && resFlag;
                }
                if (!flag) {
                    return true;
                }
            } else if (conditionStr.contains("||")) {
                String[] conArr = conditionStr.split("&&");
                boolean flag = false;
                for (String con : conArr) {
                    boolean resFlag = checkCondition(headers, con);
                    flag = flag || resFlag;
                }
                if (!flag) {
                    return true;
                }
            } else {
                boolean resFlag = checkCondition(headers, conditionStr);
                if (!resFlag) {
                    return true;
                }
            }
        }
        return true;
    }

    private boolean checkCondition(Map<String, String> headers, String con) {
        try {
            if (con.equalsIgnoreCase("none")) {//无条件
                return true;
            } else if (con.contains("}==")) {//相等
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

    @Override
    public void initialize(List<Event> events, Jedis jedis) {
        this.events = events;
        this.jedis = jedis;
    }

    @Override
    public int actionList() {
        int err = 0;
        try {
//            Pipeline pipelined = jedis.pipelined();
            for (Event event : events) {
                pipelineExecute(event, jedis);
            }
//            logger.info("actionList,events.size:" + events.size());
//            pipelined.sync();
//            pipelined.clear();
        } catch (Exception e) {
            e.printStackTrace();
            err++;
        }
        return events.size() - err;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(ComponentConfiguration conf) {

    }
}
