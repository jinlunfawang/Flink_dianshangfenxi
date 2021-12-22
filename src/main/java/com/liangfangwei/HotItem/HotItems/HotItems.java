package com.liangfangwei.HotItem.HotItems;

import com.liangfangwei.HotItem.Bean.ItemBean;
import com.liangfangwei.HotItem.Bean.ItemViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * 统计一个小时内的热门商品 5分钟更新一次
 * 输出结果:
 * 窗口结束时间：2017-11-26 12:20:00.0
 * 窗口内容：
 * NO 1: 商品ID = 2338453 热门度 = 27
 * NO 2: 商品ID = 812879 热门度 = 18
 * NO 3: 商品ID = 4443059 热门度 = 18
 * NO 4: 商品ID = 3810981 热门度 = 14
 * NO 5: 商品ID = 2364679 热门度 = 14
 * 思路：
 * 1.既然统计一个小时内 且5分钟更新一次的结果
 * 定义滑动窗口:窗口大小为1 hour,步长为5min
 * 定义增量聚合函数 拿到相同商品的此时
 * 2.定义全窗口函数 拿到窗口的截止时间
 * <p>
 * 3.状态编程 根据窗口结束时间keyBy
 * 定义定时器
 * 定时器结束后输出所有状态
 */
public class HotItems {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<String> inputPath = env.readTextFile("/Users/liangfangwei/IdeaProjects/flinkUserAnalays/data_file/UserBehavior.csv");


        //2 逻辑处理 输出数据 itemid 数量 窗口结束时间
        // 2.1 开窗 聚合
        DataStream<ItemViewCount> dataStream = inputPath.map(line -> {
            String[] split = line.split(",");
            return new ItemBean(new Long(split[0]), new Long(split[1]), new Integer(split[2]), split[3], new Long(split[4]));
        }).filter(itemBean -> "pv".equals(itemBean.getBehavior()))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ItemBean>() {
                    @Override
                    public long extractAscendingTimestamp(ItemBean itemBean) {
                        return itemBean.getTimestamp() * 1000L;
                    }
                }).keyBy("itemId")
                .timeWindow(Time.hours(1), Time.minutes(5))
                /**
                 *  结合使用
                 * 　ReduceFunction/AggregateFunction和ProcessWindowFunction结合使用，分配到某个窗口的元素将被提前聚合
                 * 而当窗口的trigger触发时，也就是窗口收集完数据关闭时，将会把聚合结果发送到ProcessWindowFunction中，这时Iterable参数将会只有一个值，就是前面聚合的值。
                 */
                .aggregate(new MyAggreateFunction(), new MyAllWinAggreateFunction());
        // 2.2保存数据状态
        SingleOutputStreamOperator<String> waterEnd = dataStream.keyBy("windowEnd").process(new TopNHotItems(5));
        waterEnd.print();
        env.execute();

    }

    /**
     * 窗口增量聚合函数 求窗口内的个数
     */
    public static class MyAggreateFunction implements AggregateFunction<ItemBean, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ItemBean itemBean, Long accumulator) {

            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    /**
     * 全窗口函数： 在增量聚合之后 使用全窗口函数  封装item_id 浏览次数 窗口结束时间
     */
    public static class MyAllWinAggreateFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {


        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {

            Long itemId = tuple.getField(0);
            long windowEnd = window.getEnd();
            Long count = input.iterator().next();
            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }

    public static class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        ListState<ItemViewCount> hotItems;
        long triggersTs = 0;
        // 定义属性，top n的大小
        private Integer topSize;

        public TopNHotItems(Integer topSize) {
            this.topSize = topSize;
        }

        /**
         * 对象创建后 先调用这个方法
         *
         * @param configuration
         * @throws Exception
         */
        @Override
        public void open(Configuration configuration) throws Exception {
            hotItems = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("hot_items", ItemViewCount.class));
        }


        /**
         * 每条数据来执行的逻辑
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
         /*   hotItems.add(value);
            if (triggersTs == 0) {
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
            }*/
            hotItems.add(value);
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);


        }

        /**
         * 到达窗结束时间 输出所有状态
         *
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            // 取出状态所有数据
            ArrayList<ItemViewCount> itemViewCounts = Lists.newArrayList(hotItems.get().iterator());
            // 按照降序排序
            itemViewCounts.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.append("===================================\n");
            stringBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < Math.min(topSize, itemViewCounts.size()); i++) {
                ItemViewCount currentItemViewCount = itemViewCounts.get(i);
                stringBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 商品ID = ").append(currentItemViewCount.getItemId())
                        .append(" 热门度 = ").append(currentItemViewCount.getCount())
                        .append("\n");
            }
            stringBuilder.append("===============================\n\n");

            // 控制输出频率
        //    Thread.sleep(2000L);

            out.collect(stringBuilder.toString());
        }


    }
}
