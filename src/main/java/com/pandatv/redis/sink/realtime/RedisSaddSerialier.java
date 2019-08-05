package com.pandatv.redis.sink.realtime;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.*;

/**
 * Created by likaiqing on 2018/5/9.
 */
public class RedisSaddSerialier implements RedisEventSerializer {
    private static final Logger logger = LoggerFactory.getLogger(RedisSaddSerialier.class);

    private static List<Event> events;
    private static Jedis jedis;

    private static int saddExpire;
    private static String saddKeys;
    private static String[] arrKey;
    private static String saddValues;
    private static String[] arrValue;

    //    private static boolean useFilterContains;
    private static boolean filterValueEmptyOrZero;

    @Override
    public void initialize(List<Event> events, Jedis jedis) {
        this.events = events;
        this.jedis = jedis;
    }

    @Override
    public int actionList() {
        int err = 0;
        Pipeline pipelined = null;
        try {
            Set<String> expireKeys = new HashSet<>();
            pipelined = jedis.pipelined();
            for (Event event : events) {
//                if (useFilterContains){
//                    String body = new String(event.getBody(), Charsets.UTF_8);
//
//                }
                pipelineExecute(event, pipelined, expireKeys);
            }
            logger.debug("actionList,events.size:" + events.size());
            if (saddExpire > 0) {
                for (String expireKey : expireKeys) {
                    pipelined.expire(expireKey, saddExpire);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            err = events.size();
        } finally {
            pipelined.sync();
            try {
                pipelined.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return events.size() - err;
    }

    private void pipelineExecute(Event event, Pipeline pipelined, Set<String> expireKeys) {
        List<String> transedKeys = getTransedKeysOrValues(event, arrKey);
        logger.debug("transedKeys:" + transedKeys);
        List<String> transedValues = getTransedKeysOrValues(event, arrValue);
        Preconditions.checkArgument(transedKeys.size() == transedValues.size(), "transedKeys.size()==transedValues.size() must,transedKeys:" + transedKeys + ";transedValues:" + transedValues);
        for (int i = 0; i < transedKeys.size(); i++) {
            String value = transedValues.get(i);
            if (filterValueEmptyOrZero && (StringUtils.isEmpty(value) || value.equals("0"))) {
                continue;
            }
            String key = transedKeys.get(i);
            pipelined.sadd(key, value);
            expireKeys.add(key);
        }
    }

    private List<String> getTransedKeysOrValues(Event event, String[] arr) {
        List<String> transed = new ArrayList<>(arr.length);
        Map<String, String> headers = event.getHeaders();
        for (String str : arr) {
            transed.add(getSingleKeyOrValue(str, headers));
        }
        return transed;
    }

    private String getSingleKeyOrValue(String str, Map<String, String> headers) {
        String transed = "";
        if (str.contains("|")) {
            transed = Arrays.stream(str.split("\\|")).reduce((a, b) -> {
                return getTransedField(a, headers) + "|" + getTransedField(b, headers);
            }).get();
        } else {
            transed = getTransedField(str, headers);
        }
        return transed;
    }

    private String getTransedField(String a, Map<String, String> headers) {
        if (a.startsWith("${")) {
            a = headers.get(a.substring(2, a.indexOf("}")));
        }
        return a;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Context context) {
        saddExpire = context.getInteger("saddExpire", 430000);
        saddKeys = context.getString("saddKeys");
        saddValues = context.getString("saddValues");
        Preconditions.checkArgument(StringUtils.isNotEmpty(saddKeys) && StringUtils.isNotEmpty(saddValues), "saddKeys and saddValues must not null");
        arrKey = saddKeys.trim().split("\\s+");
        arrValue = saddValues.trim().split("\\s+");
//        useFilterContains = context.getBoolean("useFilterContains", false);
        filterValueEmptyOrZero = context.getBoolean("filterValueEmptyOrZero", false);
    }

    @Override
    public void configure(ComponentConfiguration conf) {

    }
}
