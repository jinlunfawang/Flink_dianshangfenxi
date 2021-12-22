package com.liangfangwei.HotItem.HotPvUv;

import com.liangfangwei.HotItem.Bean.ItemBean;
import com.liangfangwei.HotItem.Bean.PageViewCount;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
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
 *
 * 再一小时的时间窗口内,每来一条数据 触发计算 。
 * 计算逻辑:
 *    1.取当前数据去redis的位图中查有没有
 *     查询的key为          时间窗口的结束时间
 *     查询的offset为       userID的hash值
 *    2. 如果没有给查询的位置 置为1
 *
 *     取判读redis的位图中有没有
 * 如果有丢弃 如果没有 count+1 将新的值存到redis中
 */


public class HotUVWithBloomFilter {
    public static void main(String[] args) throws Exception {

        // 1 执行环境

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStreamSource<String> inputStream = executionEnvironment.readTextFile("/Users/liangfangwei/IdeaProjects/flinkUserAnalays/data_file/UserBehavior.csv");
        // 2.触发计算
        SingleOutputStreamOperator<PageViewCount> itemBeanSingleOutputStreamOperator = inputStream.map(line -> {
            String[] split = line.split(",");
            return new ItemBean(new Long(split[0]), new Long(split[1]), new Integer(split[2]), split[3], new Long(split[4]));
        }).filter(itemBean -> "pv".equals(itemBean.getBehavior()))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ItemBean>() {
                    @Override
                    public long extractAscendingTimestamp(ItemBean itemBean) {
                        return itemBean.getTimestamp() * 1000L;
                    }
                    // 所有的数据开一个滚动窗口
                }).timeWindowAll(Time.hours(1))
                // 定义每个每个元素执行后续运算
                .trigger(new EveryOneEleTrigger())
                // 每个元素实际执行的逻辑
                .process(new EveryOneProcess());


        itemBeanSingleOutputStreamOperator.print();
        executionEnvironment.execute();

    }

    public static class EveryOneEleTrigger extends Trigger<ItemBean, TimeWindow> {


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

    public static class EveryOneProcess extends ProcessAllWindowFunction<ItemBean, PageViewCount, TimeWindow> {
        Jedis jedis;
        UvWithBloomFilter.MyBloomFilter myBloomFilter;

        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost", 6379);
            myBloomFilter= new UvWithBloomFilter.MyBloomFilter(1<<29);
        }

        @Override
        public void process(Context context, Iterable<ItemBean> elements, Collector<PageViewCount> out) throws Exception {
            Long currentKey = context.window().getEnd();
            ItemBean itemBean = elements.iterator().next();
            Long userId = itemBean.getUserId();

            Long place = myBloomFilter.hashCode(userId.toString(), 61);
            // 1.redis中指定位置上有没有这元素
            boolean isExist = jedis.getbit(currentKey.toString(), place);
                //2.1 没有 这个位置设置为1 并且count+1 输出
            if (!isExist) {
                jedis.setbit(currentKey.toString(), place, true);
                Long count=1L;
                String pageUVCount = jedis.hget("page_uv", currentKey.toString());
                if(StringUtils.isNoneBlank(pageUVCount)){
                    count = Long.valueOf(pageUVCount);
                }
                jedis.hset("page_uv", currentKey.toString(),String.valueOf(count+1));
                out.collect(new PageViewCount("url", currentKey, count));
            }
            //2.2 有 什么也不做

        }
    }


}
