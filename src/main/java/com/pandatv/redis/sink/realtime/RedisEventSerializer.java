package com.pandatv.redis.sink.realtime;

import org.apache.flume.Event;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurableComponent;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;

/**
 * Created by likaiqing on 2017/6/21.
 */
public interface RedisEventSerializer extends Configurable,ConfigurableComponent {

    void initialize(List<Event> events, Jedis jedis);

    /**
     * Get the actions that should be written out to hbase as a result of this
     * event. This list is written to hbase using the HBase batch API.
     *
     * @return List of {@link org.apache.hadoop.hbase.client.Row} which are
     * written as such to HBase.
     * <p>
     * 0.92 increments do not implement Row, so this is not generic.
     */

    public int actionList();

    /*
     * Clean up any state. This will be called when the sink is being stopped.
     */
    public void close();
}
