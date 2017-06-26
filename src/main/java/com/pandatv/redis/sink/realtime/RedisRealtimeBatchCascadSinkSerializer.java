package com.pandatv.redis.sink.realtime;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.*;

/**
 * Created by likaiqing on 2017/6/23.
 */
public class RedisRealtimeBatchCascadSinkSerializer implements RedisEventSerializer {
    private static final Logger logger = LoggerFactory.getLogger(RedisRealtimeBatchCascadSinkSerializer.class);
    private static final String redisKeySep = "-";
    private static Set<String> minuteFields = new HashSet<>();
    private static Set<String> parDates = new HashSet<>();
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
            logger.info("actionList,events.size:" + events.size());
            pipelined.sync();
            pipelined.clear();
            logger.info("pipelineExecute over,parDates.size():" + parDates.size() + ";minutes.size():" + minuteFields.size());
            if (hsetCascadHset && timeHelper.checkout()) {
                for (String field : minuteFields) {
                    logger.info("executeCascadHset,field:" + field);
                    executeCascadHset(field, jedis);
                }
                minuteFields.clear();
                parDates.clear();
            }

        } catch (Exception e) {
            e.printStackTrace();
            err++;
        }
        return events.size() - err;
    }

    private void executeCascadHset(String field, Jedis jedis) {
        if (parDates.size() == 1) {
            logger.info("parDates.size==" + parDates.size());
            String parDate = Lists.newArrayList(parDates).get(0);
            logger.info("executeHset,field=" + field + ";parDate:" + parDate);
            executeHset(field, parDate, jedis);
        } else if (parDates.size() == 2) {
            logger.info("parDates.size==" + parDates.size());
            ArrayList<String> parDateList = Lists.newArrayList(parDates);
            String first = parDateList.get(0);
            String seconde = parDateList.get(1);
            String little = first.compareTo(seconde) < 0 ? first : seconde;
            String big = first.compareTo(seconde) > 0 ? first : seconde;
            if (field.startsWith("0")) {//新的一天,匹配big
                logger.info("executeHset,field=" + field + ";parDate:" + big);
                executeHset(field, big, jedis);
            } else {//昨天,匹配little
                logger.info("executeHset,field=" + field + ";parDate:" + little);
                executeHset(field, little, jedis);
            }
        } else {
            logger.error("parDates.size<0 or parDates.size>=3");
        }
    }

    private void executeHset(String field, String parDate, Jedis jedis) {
        String minuteKey = new StringBuffer(hsetKeyPrefix).append(field).append(redisKeySep).append(hsetKeyName).append(redisKeySep).append(hsetKeySuffix).toString();
        String newKey = new StringBuffer(hsetKeyPrefix).append(parDate).append(redisKeySep).append(hsetHashKeyName).append(redisKeySep).append(hsetKeySuffix).toString();
        int total = 0;
        logger.info("minuteKey:" + minuteKey + ";newKey:" + newKey);
        for (String value : jedis.hvals(minuteKey)) {
            try {
                total = total + Integer.parseInt(value);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        logger.info("jedis.hset,newKey:" + newKey + ";field:" + field + ";total:" + total);
        jedis.hset(newKey, field, total + "");
    }

    private void pipelineExecute(Event event, Pipeline pipelined) {
        Map<String, String> headers = event.getHeaders();
        String key = getKey(headers);
        String field = getField(headers);
        String value = getValue(headers);
        minuteFields.add(headers.get(hsetKeyPreVar.substring(2, hsetKeyPreVar.length() - 1)));
        parDates.add(headers.get("par_date"));
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
        return new StringBuffer(hsetKeyPrefix).append(headers.get(hsetKeyPreVar.substring(2, hsetKeyPreVar.length() - 1))).append(redisKeySep).append(hsetKeyName).append(redisKeySep).append(hsetKeySuffix).toString();
    }

    class TimeHelper {
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

    @Override
    public void close() {

    }

    @Override
    public void configure(Context context) {
        hset = context.getBoolean("hset", false);
        hsetExpire = context.getInteger("hsetExpire", 3600);
        hsetKeyPrefix = context.getString("hsetKeyPrefix");
        if (StringUtils.isNotEmpty(hsetKeyPrefix) && !hsetKeyPrefix.endsWith(redisKeySep)) {
            hsetKeyPrefix = hsetKeyPrefix + redisKeySep;
        }
        hsetKeyPreVar = context.getString("hsetKeyPreVar");
        hsetKeyName = context.getString("hsetKeyName");
        hsetKeySuffix = context.getString("hsetKeySuffix");
        hsetField = context.getString("hsetField");
        hsetValue = context.getString("hsetValue");
        hsetCascadHset = context.getBoolean("hsetCascadHset", false);
        Long setCascadHsetTime = context.getLong("setCascadHsetTime", 45000l);
        timeHelper = new TimeHelper(setCascadHsetTime);
        hsetHashKeyName = context.getString("hsetHashKeyName");
        hsetHashKeyPreVar = context.getString("hsetHashKeyPreVar");
    }

    @Override
    public void configure(ComponentConfiguration conf) {

    }
}
