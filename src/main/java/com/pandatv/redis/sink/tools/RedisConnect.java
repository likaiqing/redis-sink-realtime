package com.pandatv.redis.sink.tools;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by likaiqing on 2017/6/21.
 */
public class RedisConnect {
    private static JedisPool pool;
    private Jedis jedis;
    private String url = "";
    private static final Logger logger = LoggerFactory.getLogger(RedisConnect.class);

    public RedisConnect(String host, int port) {
        init(host, port);
    }

    private void init(String host, int port) {
        if (pool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(50);
            config.setMaxWaitMillis(5);
            pool = new JedisPool(config, host, port);
        }
    }

    public Jedis getRedis(String pwd) {
        Jedis jedis = pool.getResource();
        if (!StringUtils.isEmpty(pwd)) {
            jedis.auth(pwd);
        }
        return jedis;
    }

    public void close() {
        if (null != pool){
            pool.destroy();
        }
    }
}
