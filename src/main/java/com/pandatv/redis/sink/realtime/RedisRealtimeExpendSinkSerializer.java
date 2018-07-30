package com.pandatv.redis.sink.realtime;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.pandatv.redis.sink.constant.RedisSinkConstant;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by likaiqing on 2017/6/23.
 */
public class RedisRealtimeExpendSinkSerializer implements RedisEventSerializer {
    private static final Logger logger = LoggerFactory.getLogger(RedisRealtimeExpendSinkSerializer.class);
    private Set<String> saddMinuteNameFields = new HashSet<>();
    //    private static Set<String> hincrMinuteNameFields = new HashSet<>();
    private HashMultimap<String, Integer> hincrMinuteNameMap = HashMultimap.create();
    private List<Event> events;
    private Jedis jedis;

    private String keySep = ":";

    private String condition = null;

    private boolean hincrby = false;
    private String hincrbyKeyPrefix;
    private String hincrbyKeyPreVar;
    private String hincrbyKeyName;
    private String hincrbyKeySuffix;
    private String hincrbyField;
    private String hincrbyValue;

    private boolean sadd = false;
    private int saddExpire;
    private String saddKeyPrefix;
    private String saddKeyPreVar;
    private String saddKeyName;
    private String saddKeySuffix;
    private String saddValue;
    private boolean saddCascadHset = false;
    private String saddHashKeyPreVar;
    private String saddHashKeyName;
    private Map<String, String> platMap = new HashedMap();
    private String mysqlUrl;
    private String mysqlUser;
    private String mysqlPass;
    private boolean useMysql;

    private Map<String, String> roomClaMap;
    private Connection con = null;
    private Statement stmt = null;
    private ResultSet rs = null;
    //消费日志中是主播的qid,非房间号
    private String dbSqlPre = "select hostid,classification from room where hostid in (";

    private boolean saddClassificationCascad = false;
    private boolean hincrbyClassificationCascad = false;

    private void initMysqlConn() {
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

    public void setRoomClamap(String dbSql) {
        try {
            if (con.isClosed() || con == null || stmt.isClosed() || stmt == null) {
                initMysqlConn();
            }
            rs = stmt.executeQuery(dbSql);
            Map<String, String> newRoomClaMap = new HashedMap();
            while (rs.next()) {
                newRoomClaMap.put(rs.getString(1), rs.getString(2));
            }
            roomClaMap = newRoomClaMap;
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
    }

    @Override
    public int actionList() {
        int err = 0;
        try {
            if (useMysql) {
                String roomIds = events.stream().map(e -> e.getHeaders().get("anchor_id")).distinct().reduce((a, b) -> new StringBuffer(a).append(",").append(b).toString()).get();
                setRoomClamap(new StringBuffer(dbSqlPre).append(roomIds).append(")").toString());
            }
            Pipeline pipelined = jedis.pipelined();
            for (Event event : events) {
                pipelineExecute(event, pipelined);
            }
            pipelined.sync();
            pipelined.clear();
            if (saddCascadHset) {
                Map<String, String> piplineMap = new HashedMap();
                for (String field : saddMinuteNameFields) {
                    executeCascadHset(field, jedis, piplineMap);
                }
                Pipeline newPipeline = jedis.pipelined();
                for (Map.Entry<String, String> entry : piplineMap.entrySet()) {
                    String[] key = entry.getKey().split(keySep);
                    String value = entry.getValue();
                    newPipeline.hset(key[0], key[1], value);
                }
                newPipeline.sync();
                newPipeline.clear();
                saddMinuteNameFields.clear();
            }
        } catch (Exception e) {
            e.printStackTrace();
            err++;
        }
        return events.size() - err;
    }

    private void executeCascadHset(String field, Jedis jedis, Map<String, String> piplineMap) {
        String parDate = field.substring(0, 8);
        String minute = field.substring(0, field.indexOf(RedisSinkConstant.redisKeySep));
        String name = field.substring(field.indexOf(RedisSinkConstant.redisKeySep) + 1, field.lastIndexOf(RedisSinkConstant.redisKeySep));
        String suffix = field.substring(field.lastIndexOf(RedisSinkConstant.redisKeySep) + 1);
        String minuteNameKey = new StringBuffer(saddKeyPrefix).append(field).toString();
        String newKey = new StringBuffer(saddKeyPrefix).append(parDate).append(RedisSinkConstant.redisKeySep).append(name).append(RedisSinkConstant.redisKeySep).append(suffix).toString();
        Long uv = jedis.scard(minuteNameKey);
        piplineMap.put(new StringBuffer(newKey).append(keySep).append(minute).toString(), String.valueOf(uv));
//        jedis.hset(newKey, minute, String.valueOf(uv));
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
        if (!condition.equalsIgnoreCase("none")) {
            Map<String, String> headers = event.getHeaders();
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
            String name = getParamValue(headers, saddKeyNameNameArr[i]);
            String suffix = saddKeySuffixArr[i];
            String value = getParamValue(headers, saddValueArr[i]);
            String key = getSaddKey(headers, name, suffix);
            saddMinuteNameFields.add(new StringBuffer(headers.get(saddKeyPreVar.substring(2, saddKeyPreVar.length() - 1))).append(RedisSinkConstant.redisKeySep).append(name).append(RedisSinkConstant.redisKeySep).append(suffix).toString());
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
            String name = getParamValue(headers, hincrbyKeyNameArr[i].trim());
            String suffix = hincrbyKeySuffixArr[i];
            String field = getParamValue(headers, hincrbyFieldArr[i].trim());
            int value = 1;
            try {
                value = Integer.parseInt(getParamValue(headers, hincrbyValueArr[i].trim()));
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (suffix.contains("anchor")) {
                hincrMinuteNameMap.put(new StringBuffer(field).append(RedisSinkConstant.redisKeySep).append(name).append(RedisSinkConstant.redisKeySep).append(suffix).toString(), value);
//                hincrMinuteNameFields.add(new StringBuffer(field).append(RedisSinkConstant.redisKeySep).append(name).append(RedisSinkConstant.redisKeySep).append(suffix).toString());
            }
            String hincrKey = getHincrKey(headers, name, suffix);
            pipelined.hincrBy(hincrKey, field, value);
        }
    }

    private String getParamValue(Map<String, String> headers, String keyName) {
        if (keyName.contains(RedisSinkConstant.redisKeySep)) {
            return Arrays.stream(keyName.split("-")).map(name -> getParamValue(headers, name)).reduce((a, b) -> a + "-" + b).get();
        } else if (keyName.contains("base64.encode")) {
            return Base64.getEncoder().encodeToString(headers.get(keyName.substring(keyName.indexOf("{") + 1, keyName.length() - 1)).getBytes());
        } else if (keyName.contains("${")) {
            String result = headers.get(keyName.substring(2, keyName.length() - 1));
            result = platMap.containsKey(result) ? platMap.get(result) : result;
            return result;
        } else if (keyName.contains("$[")) {
            return roomClaMap.get(headers.get(keyName.substring(2, keyName.length() - 1)));
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
        if (hincrby) {
            hincrbyKeyPrefix = context.getString("hincrbyKeyPrefix");
            if (StringUtils.isNotEmpty(hincrbyKeyPrefix) && !hincrbyKeyPrefix.endsWith(RedisSinkConstant.redisKeySep)) {
                hincrbyKeyPrefix = hincrbyKeyPrefix + RedisSinkConstant.redisKeySep;
            }
            hincrbyKeyPreVar = context.getString("hincrbyKeyPreVar");
            hincrbyKeyName = context.getString("hincrbyKeyName");
            Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyKeyName), "hincrbyKeyName must ");
            hincrbyKeySuffix = context.getString("hincrbyKeySuffix");
            Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyKeySuffix), "hincrbyKeySuffix must ");
            hincrbyField = context.getString("hincrbyField");
            Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyField), "hincrbyField must ");
            hincrbyValue = context.getString("hincrbyValue");
            Preconditions.checkArgument(StringUtils.isNotEmpty(hincrbyValue), "hincrbyValue must ");
        }

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
        String platMapStr = context.getString("platMap", "minute");
        if (platMapStr.contains(":")) {
            platMap = Splitter.on(";").omitEmptyStrings().trimResults().withKeyValueSeparator(":").split(platMapStr);
        }
        mysqlUrl = context.getString("mysqlUrl");
        mysqlUser = context.getString("mysqlUser");
        mysqlPass = context.getString("mysqlPass");
        useMysql = context.getBoolean("useMysql", true);

        if (useMysql) {
            initMysqlConn();
        }

        saddClassificationCascad = context.getBoolean("saddClassificationCascad", false);
        hincrbyClassificationCascad = context.getBoolean("hincrbyClassificationCascad", false);
    }

    @Override
    public void configure(ComponentConfiguration conf) {

    }

}
