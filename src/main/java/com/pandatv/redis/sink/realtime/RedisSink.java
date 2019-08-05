package com.pandatv.redis.sink.realtime;

import com.google.common.base.Preconditions;
import com.pandatv.redis.sink.constant.RedisSinkConstant;
import com.pandatv.redis.sink.tools.RedisConnect;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.instrumentation.kafka.KafkaSinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by likaiqing on 2017/6/21.
 */
public class RedisSink extends AbstractSink implements Configurable {
    private static final Logger logger = LoggerFactory.getLogger(RedisSink.class);

    private SinkCounter counter;

    private int batchSize;
    private static Context redisContext = new Context();
    private static String host = null;
    private static int port = 6379;
    private static String pwd = null;

    private static RedisConnect redisConnect = null;
    private static Jedis jedis = null;

    private RedisEventSerializer serializer;
    private String eventSerializerType;
    private Context serializerContext;

    @Override
    public synchronized void start() {
        redisConnect = new RedisConnect(host, port);
        counter.start();
        super.start();
    }

    @Override
    public void configure(Context context) {
        batchSize = context.getInteger(RedisSinkConstant.BATCH_SIZE, RedisSinkConstant.DEFAULT_BATCH_SIZE);
        logger.debug("Using batch size: {}", batchSize);
        redisContext.putAll(context.getSubProperties("redis."));
        host = redisContext.getString("host");
        port = Integer.parseInt(redisContext.getString("port", "6379"));
        pwd = redisContext.getString("pwd", "");
        Preconditions.checkArgument(StringUtils.isNotEmpty(host), "redis host must ");

        if (counter == null) {
            counter = new KafkaSinkCounter(getName());
        }

        eventSerializerType = context.getString(
                RedisSinkConstant.PROPERTY_PREFIX);
        logger.info("eventSerializerType:" + eventSerializerType);
        if (eventSerializerType == null || eventSerializerType.isEmpty()) {
            eventSerializerType =
                    "org.apache.flume.sink.redis.SimpleRedisEventSerializer";
            logger.debug("No serializer defined, Will use default");
        }
        serializerContext = new Context();
        serializerContext.putAll(context.getSubProperties("serializer."));
        try {
            Class<? extends RedisEventSerializer> clazz = (Class<? extends RedisEventSerializer>) Class.forName(eventSerializerType);
            serializer = clazz.newInstance();
            serializer.configure(serializerContext);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = null;
        Event event = null;
        Jedis jedis = redisConnect.getRedis(pwd);
        int success = 0;
        int fail = 0;
        int processedEvents = 0;
        try {
            transaction = channel.getTransaction();
            transaction.begin();
            List<Event> events = new ArrayList<>();
            for (; processedEvents < batchSize; processedEvents++) {
                event = channel.take();
                if (event == null) {
                    break;
                }
                events.add(event);
            }
            if (events != null && events.size() > 0) {
                serializer.initialize(events, jedis);
                int successCnt = serializer.actionList();
                counter.addToEventDrainSuccessCount(successCnt);
                success += successCnt;
                counter.addToEventDrainAttemptCount(batchSize - successCnt);
                fail = batchSize - successCnt;
            }
            logger.debug("send message :" + success);
            logger.debug("failed message :" + fail);
            transaction.commit();
        } catch (Throwable th) {
            transaction.rollback();
            logger.error("process failed", th);
            if (th instanceof Error) {
                throw (Error) th;
            } else {
                throw new EventDeliveryException(th);
            }
        } finally {
            if (transaction != null) {
                transaction.close();
            }
            if (jedis != null) {
                jedis.close();
            }

        }
        return result;
    }


    @Override
    public synchronized void stop() {
        redisConnect.close();
        counter.stop();
        logger.debug("Kafka Sink {} stopped. Metrics: {}", getName(), counter);
        super.stop();
    }
}
