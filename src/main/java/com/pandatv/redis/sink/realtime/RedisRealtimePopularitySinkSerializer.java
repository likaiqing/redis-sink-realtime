package com.pandatv.redis.sink.realtime;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.pandatv.redis.sink.constant.RedisSinkConstant;
import com.pandatv.redis.sink.tools.TimeHelper;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.sql.*;
import java.util.*;
import java.util.stream.IntStream;

/**
 * Created by likaiqing on 2017/6/23.
 */
public class RedisRealtimePopularitySinkSerializer implements RedisEventSerializer {
    private static final Logger logger = LoggerFactory.getLogger(RedisRealtimePopularitySinkSerializer.class);
    private static Set<String> minuteFields = new HashSet<>();
    //    private static Set<String> minuteRoomIdFields = new HashSet<>();
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
    private static String mysqlUrl;
    private static String mysqlUser;
    private static String mysqlPass;

    private static Map<String, String> roomClaMap;
    private static Connection con = null;
    private static Statement stmt = null;
    private static ResultSet rs = null;
    private static String dbSqlPre = "select id,classification from room where id in (";

    private static String hsetClassificationKeySuffix;
    private static String hsetClassificationKeyName;

    private static long minCurClassiCastMinute = 0;
    private static long maxCurClassiCastMinute = 0;
    private static final String redisMinuteKey = "rt_pcu-minute";

    DateTimeFormatter stf = DateTimeFormat.forPattern("yyyyMMddHHmm");

    private static void initMysqlConn() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            if (null == con || con.isClosed()) {
                con = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPass);
                logger.info("DriverManager.getConnection,mysqlUrl:{}", mysqlUrl);
            }
            if (null == stmt || stmt.isClosed()) {
                stmt = con.createStatement();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void setRoomClamap(String dbSql) {
        try {
            if (con.isClosed() || con == null || stmt.isClosed() || stmt == null) {
                initMysqlConn();
            }
            rs = stmt.executeQuery(dbSql);
            roomClaMap = new HashMap<>();
            while (rs.next()) {
                String roomId = rs.getString(1);
                String classi = rs.getString(2);
                roomClaMap.put(roomId, classi);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (!rs.isClosed() || rs == null) {
                    rs.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void initialize(List<Event> events, Jedis jedis) {
        this.events = events;
        this.jedis = jedis;
        /**
         * 如果没有,则当天的凌晨零分
         */
        if (minCurClassiCastMinute == 0) {
            String minute = jedis.get(redisMinuteKey);
            if (StringUtils.isEmpty(minute)) {
                minCurClassiCastMinute = Long.parseLong(DateTimeFormat.forPattern("yyyyMMdd0000").print(new DateTime()));
                jedis.set(redisMinuteKey, String.valueOf(minCurClassiCastMinute));
            } else {
                minCurClassiCastMinute = Long.parseLong(minute);
            }
        }
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
        } catch (Exception e) {
            e.printStackTrace();
            err++;
        }
        try {
            if (timeHelper.checkout()) {
                hsetCascad(jedis);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("");
        }
        return events.size() - err;
    }

    /**
     * 级联操作,全量和版区的PCU统计,maxCurClassiCastMinute不做统计,处理之后minCurClassiCastMinute = maxCurClassiCastMinute,下一个超时处理
     *
     * @param jedis
     */
    private void hsetCascad(Jedis jedis) {
        for (; minCurClassiCastMinute < maxCurClassiCastMinute; ) {
            Map<String, String> roomPcuMap = null;
            int i = 3;
            while ((roomPcuMap == null || roomPcuMap.size() == 0) && i >= 0) {
                long newMinute = Long.parseLong(stf.print(stf.parseDateTime(String.valueOf(minCurClassiCastMinute)).plusMinutes(-i)));
                roomPcuMap = getRoomPcuMap(newMinute);
                logger.info("newMinute:" + newMinute + ",minCurClassiCastMinute:" + minCurClassiCastMinute + ",roomPcuMap.size:" + roomPcuMap.size() + ",i:" + i);
                i--;
            }
            if (roomPcuMap.size() > 0) {
                String anchorIds = Joiner.on(",").join(roomPcuMap.keySet());
                Map<String, Integer> classiPcuMap = new HashedMap();
                setRoomClamap(new StringBuffer(dbSqlPre).append(anchorIds).append(")").toString());
                for (Map.Entry<String, String> entry : roomPcuMap.entrySet()) {
                    String roomId = entry.getKey();
                    int pcu = 0;
                    try {
                        pcu = Integer.parseInt(entry.getValue());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    String classi = roomClaMap.get(roomId);
                    classiPcuMap.merge(classi, pcu, (oldV, newV) -> oldV + newV);
                }
                Pipeline pipelined = jedis.pipelined();
                for (Map.Entry<String, Integer> entry : classiPcuMap.entrySet()) {
                    String newkey = new StringBuffer(hsetKeyPrefix).append(String.valueOf(minCurClassiCastMinute).substring(0, 8)).append(RedisSinkConstant.redisKeySep).append(hsetClassificationKeyName).append(RedisSinkConstant.redisKeySep).append(entry.getKey()).append(RedisSinkConstant.redisKeySep).append(hsetClassificationKeySuffix).toString();
                    pipelined.hset(newkey, String.valueOf(minCurClassiCastMinute), String.valueOf(entry.getValue()));
                }
                pipelined.sync();
                pipelined.clear();
                String parDate = String.valueOf(minCurClassiCastMinute).substring(0, 8);
                String newKey = new StringBuffer(hsetKeyPrefix).append(parDate).append(RedisSinkConstant.redisKeySep).append(hsetHashKeyName).append(RedisSinkConstant.redisKeySep).append(hsetKeySuffix).toString();
                int total = 0;
                for (String value : roomPcuMap.values()) {
                    try {
                        total = total + Integer.parseInt(value);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                jedis.hset(newKey, String.valueOf(minCurClassiCastMinute), total + "");
                minCurClassiCastMinute = Long.parseLong(stf.print(stf.parseDateTime(String.valueOf(minCurClassiCastMinute)).plusMinutes(1)));
            }
        }
        jedis.set(redisMinuteKey, String.valueOf(minCurClassiCastMinute));
    }

    private Map<String, String> getRoomPcuMap(long minute) {
        String tmpHashKey = new StringBuffer(hsetKeyPrefix).append(minute).append(RedisSinkConstant.redisKeySep).append(hsetKeyName).append(RedisSinkConstant.redisKeySep).append(hsetKeySuffix).toString();
        return jedis.hgetAll(tmpHashKey);
    }

    private void hsetClassificationCascad(Jedis jedis) {
        if (maxCurClassiCastMinute - minCurClassiCastMinute > 3) {
            String tmpHashKey = new StringBuffer(hsetKeyPrefix).append(minCurClassiCastMinute).append(RedisSinkConstant.redisKeySep).append(hsetKeyName).append(RedisSinkConstant.redisKeySep).append(hsetKeySuffix).toString();
            String anchorIds = Joiner.on(",").join(jedis.hkeys(tmpHashKey));
//            logger.info("tmpHashKey:{},anchorIds=jedis.hkeys(tmpHashKey),anchorIds:{}", tmpHashKey, anchorIds);
            if (StringUtils.isNotEmpty(anchorIds)) {
                Map<String, Integer> classiPcuMap = new HashedMap();
                setRoomClamap(new StringBuffer(dbSqlPre).append(anchorIds).append(")").toString());
                for (Map.Entry<String, String> entry : jedis.hgetAll(tmpHashKey).entrySet()) {
                    String roomId = entry.getKey();
                    int pcu = 0;
                    try {
                        pcu = Integer.parseInt(entry.getValue());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    String classi = roomClaMap.get(roomId);
                    classiPcuMap.merge(classi, pcu, (oldV, newV) -> oldV + newV);
                }
                Pipeline pipelined = jedis.pipelined();
                for (Map.Entry<String, Integer> entry : classiPcuMap.entrySet()) {
                    String newkey = new StringBuffer(hsetKeyPrefix).append(String.valueOf(minCurClassiCastMinute).substring(0, 8)).append(RedisSinkConstant.redisKeySep).append(hsetClassificationKeyName).append(RedisSinkConstant.redisKeySep).append(entry.getKey()).append(RedisSinkConstant.redisKeySep).append(hsetClassificationKeySuffix).toString();
                    String value = String.valueOf(entry.getValue());
                    pipelined.hset(newkey, String.valueOf(minCurClassiCastMinute), String.valueOf(entry.getValue()));
//                    logger.info("pipelined.hset,newKey:{},value:{}", newkey, value);
                }
                pipelined.sync();
                pipelined.clear();
            }
        }
        minCurClassiCastMinute = Long.parseLong(stf.print(stf.parseDateTime(String.valueOf(minCurClassiCastMinute)).plusMinutes(1)));
        if (maxCurClassiCastMinute - minCurClassiCastMinute >= 2) {
            logger.debug("continue execute hsetClassificationCascad()");
            hsetClassificationCascad(jedis);
        }
    }

    private void executeCascadHset(String field) {
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

    /**
     * 处理单个event,设置每个房间的值,超时
     * 一直保留最大值和最小值,存入redis,每次超时处理后,将最值重新写入redis
     * 根据当前event的时间生成当前及前三分钟的key,均hset
     *
     * @param event
     * @param pipelined
     */
    private void pipelineExecute(Event event, Pipeline pipelined) {
        Map<String, String> headers = event.getHeaders();
        String minute = headers.get(hsetKeyPreVar.substring(2, hsetKeyPreVar.length() - 1));
        String field = getField(headers);//room_id
        String value = getValue(headers);
        /**
         * 1,kafka发送的数据可能会出现一分钟多都没有数据,如果这种情况出现,会有后面两分钟的数据继续往当前分钟减去3的分钟插入数据,超时处理的时候避免了没有数据的情况,最多不能超过两分钟都没有数据
         * 2,避免重启时间过长,设置超时为5分钟,如果5分钟重启失败,不能保证重启时读到的第一分钟数据准确性
         */
        IntStream.range(-3, 0).mapToObj(i -> getKey(headers, minute, i)).forEach(key -> {
            pipelined.hset(key, field, value);
            pipelined.expire(key, 300);
        });
        try {
            long minuteLong = Long.parseLong(minute);
            if (minuteLong < minCurClassiCastMinute) {
                minCurClassiCastMinute = minuteLong;
            }
            if (minuteLong > maxCurClassiCastMinute) {
                maxCurClassiCastMinute = minuteLong;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private String getValue(Map<String, String> headers) {
        String value = hsetValue;
        if (hsetValue.contains("${")) {
            value = headers.get(hsetValue.substring(2, hsetValue.length() - 1));
        }
        return value;
    }

    /**
     * header中获取room_id值
     *
     * @param headers
     * @return
     */
    private String getField(Map<String, String> headers) {
        String field = hsetField;
        if (hsetField.contains("${")) {
            field = headers.get(hsetField.substring(2, hsetField.length() - 1));
        }
        return field;
    }

    /**
     * 获取当前event分钟前n分钟的map结果key
     */
    public String getKey(Map<String, String> headers, String originMinute, int plusMinutes) {
        String newMinute = stf.print(stf.parseDateTime(String.valueOf(originMinute)).plusMinutes(plusMinutes));
        return new StringBuffer(hsetKeyPrefix).append(newMinute).append(RedisSinkConstant.redisKeySep).append(hsetKeyName).append(RedisSinkConstant.redisKeySep).append(hsetKeySuffix).toString();
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
        mysqlUrl = context.getString("mysqlUrl");
        mysqlUser = context.getString("mysqlUser");
        mysqlPass = context.getString("mysqlPass");

        hsetClassificationKeySuffix = context.getString("hsetClassificationKeySuffix", "classi_pcu");
        hsetClassificationKeyName = context.getString("hsetClassificationKeyName", "minute");
        initMysqlConn();
    }

    @Override
    public void configure(ComponentConfiguration conf) {

    }
}
