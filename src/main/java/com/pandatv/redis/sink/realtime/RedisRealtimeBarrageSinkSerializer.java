package com.pandatv.redis.sink.realtime;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.List;

/**
 * Created by likaiqing on 2017/6/23.
 */
public class RedisRealtimeBarrageSinkSerializer implements RedisEventSerializer  {
    private static final Logger logger = LoggerFactory.getLogger(RedisRealtimeBarrageSinkSerializer.class);

    private static List<Event> events;
    private static Jedis jedis;


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

        } catch (Exception e) {
            e.printStackTrace();
            err++;
        }
        return events.size() - err;
    }

    private void pipelineExecute(Event event, Pipeline pipelined) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Context context) {

    }

    @Override
    public void configure(ComponentConfiguration conf) {

    }
}
