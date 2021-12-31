package com.liangfangwei.pvuv;

import com.liangfangwei.bean.ItemBean;
import com.liangfangwei.bean.PageViewCount;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;


/**
 * @author :LiangFangWei
 * @date: 2021-12-21 15:55
 * -                   _ooOoo_
 * -                  o8888888o
 * -                  88" . "88
 * -                  (| -_- |)
 * -                   O\ = /O
 * -               ____/`---'\____
 * -             .   ' \\| |// `.
 * -              / \\||| : |||// \
 * -            / _||||| -:- |||||- \
 * -              | | \\\ - /// | |
 * -            | \_| ''\---/'' | |
 * -             \ .-\__ `-` ___/-. /
 * -          ___`. .' /--.--\ `. . __
 * -       ."" '< `.___\_<|>_/___.' >'"".
 * -      | | : `- \`.;`\ _ /`;.`/ - ` : | |
 * -        \ \ `-. \_ __\ /__ _/ .-` / /
 * ======`-.____`-.___\_____/___.-`____.-'======
 * .............................................
 * -          佛祖保佑             永无BUG
 * <p>
 * 需求：实时输出统计每个小时内的的uv。每个小时内用户去重数实时输出
 * 思路：
 * <p>
 * 再一小时的时间窗口内,每来一条数据 触发计算 。
 * 计算逻辑:
 * 1.取当前数据去redis的位图中查有没有
 * 查询的key为          时间窗口的结束时间
 * 查询的offset为       userID的hash值
 * 2. 如果没有给查询的位置 置为1
 * <p>
 * 取判读redis的位图中有没有
 * 如果有丢弃 如果没有 count+1 将新的值存到redis中
 */


public class HotUVWithBloomFilter {
    public static void main(String[] args) throws Exception {
        //1.环境准备
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        executionEnvironment.setParallelism(1);
        // 2. 准备数据

        DataStreamSource<String> inputStream = executionEnvironment.readTextFile("/Users/liangfangwei/IdeaProjects/flinkUserAnalays/data_file/UserBehavior.csv");
        SingleOutputStreamOperator<ItemBean> filterData = inputStream.map(line -> {
            String[] split = line.split(",");
            return new ItemBean(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ItemBean>() {
            @Override
            public long extractAscendingTimestamp(ItemBean element) {
                return element.getTimestamp() * 1000L;
            }
        }).filter(itemBean -> "pv".equals(itemBean.getBehavior()));

        //2.滚动窗口为1小时
        SingleOutputStreamOperator<PageViewCount> streamOperator = filterData
                .timeWindowAll(Time.hours(1))
      //3.定义触发器 需要定义每来一条数据触发计算 而不是等全部的窗口再触发计算
                .trigger(new UVTriigger())
      // 4 计算逻辑 去redis的位图查是否有没有当前userID
                .process(new UVProcessFunction());
        // 5 如果没有则 需要插入进去
        streamOperator.print();
        executionEnvironment.execute();
    }

    /**
     * 定义静态内部类 不需要将类的定义额外写在class文件中
     */
    public static class UVTriigger extends Trigger<ItemBean, TimeWindow> {
        @Override
        public TriggerResult onElement(ItemBean element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }

    public static class UVProcessFunction extends ProcessAllWindowFunction<ItemBean, PageViewCount, TimeWindow> {
        private Jedis jedis;
        private String pageCountKey = "uv_page_count";
        private BloomFilter bloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost", 6379);
            bloomFilter = new BloomFilter(1 << 29);
        }


        /**
         * 来一条数据去redis中查
         *
         * @param context
         * @param elements
         * @param out
         * @throws Exception
         */
        @Override
        public void process(Context context, Iterable<ItemBean> elements, Collector<PageViewCount> out) throws Exception {
            Long windowEnd1 = context.window().getEnd();
            String windowEnd = windowEnd1.toString();
            ItemBean itemBean = elements.iterator().next();
            Long userId = itemBean.getUserId();
            long offset = bloomFilter.hash(userId.toString(), 61);

            Boolean isExist = jedis.getbit(windowEnd, offset);
            if (!isExist) {
                jedis.setbit(windowEnd, offset, true);
                // count值+1 cont值存储为hash结构
                Long uvCount = 0L;    // 初始count值

                String uvCountString = jedis.hget(pageCountKey, windowEnd);
                if (StringUtils.isNoneBlank(uvCountString)) {
                    uvCount = Long.valueOf(uvCountString);

                }
                jedis.hset(pageCountKey, windowEnd, String.valueOf(uvCount + 1));
                out.collect(new PageViewCount("uv", windowEnd1, uvCount + 1));


            }

        }
    }

    public static class BloomFilter {
        // 要去2的幂次方 result&(capacity-1) 才是求余的
        private long capacity;

        public BloomFilter(long capacity) {
            this.capacity = capacity;
        }

        public long hash(String userId, int seed) {
            long result = 0L;
            for (int i = 0; i < userId.length(); i++) {
                result = result * seed + userId.charAt(i);
            }

            return result & (capacity - 1);
        }

    }
}