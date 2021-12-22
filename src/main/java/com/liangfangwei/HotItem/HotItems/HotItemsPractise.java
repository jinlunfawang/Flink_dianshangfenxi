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
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
 * 做什么 :统计一小时内热门商品 5分钟更新一次结果
 * 怎么做：
 * 1.既然输出1小时内商品信息,即输出历史数据,且每隔5分钟触发一次 即到达窗口结束的时候触发一次
 * 输出5分钟内保存的状态信息
 * 输出: 窗口结束时间 商品ID 热门数
 * <p>
 * 2 那么就要统计数出商品结束时间 商品ID 热门数
 * 热门数：增量聚合函数
 * 结束时间+商品ID：全窗口
 * <p>
 * 输出结果:
 * 窗口结束时间：2017-11-26 12:20:00.0
 * 窗口内容：
 * NO 1: 商品ID = 2338453 热门度 = 27
 * NO 2: 商品ID = 812879 热门度 = 18
 * NO 3: 商品ID = 4443059 热门度 = 18
 * NO 4: 商品ID = 3810981 热门度 = 14
 * NO 5: 商品ID = 2364679 热门度 = 14
 */
public class HotItemsPractise {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.readTextFile("/Users/liangfangwei/IdeaProjects/flinkUserAnalays/data_file/UserBehavior.csv");
        // 准备数据源
        DataStream<ItemBean> filterStream = inputStream.map(line -> {
            String[] split = line.split(",");
            return new ItemBean(Long.parseLong(split[0]), Long.parseLong(split[1]), Integer.parseInt(split[2]), split[3], Long.parseLong(split[4]));

        }).filter(item -> "pv".equals(item.getBehavior()));

        DataStream<ItemViewCount> windowsResult = filterStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ItemBean>() {
            @Override
            public long extractAscendingTimestamp(ItemBean element) {
                return element.getTimestamp() * 1000L;
            }
        }).keyBy("itemId").timeWindow(Time.hours(1), Time.minutes(5)).aggregate(new MyAggreateCount(), new MyAllWindowsView());
        SingleOutputStreamOperator<String> windowEnd = windowsResult.keyBy("windowEnd").process(new ItemHotTopN(5));
        windowEnd.print();

        env.execute("HotItemsPractise");
    }

    public static class MyAggreateCount implements AggregateFunction<ItemBean, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ItemBean value, Long accumulator) {
            return accumulator + 1L;
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

    public static class MyAllWindowsView implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

        /**
         * @param tuple
         * @param window
         * @param input
         * @param out
         * @throws Exception
         */
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
            long windowEnd = window.getEnd();
            long count = input.iterator().next();
            long itemId = tuple.getField(0);

            out.collect(new ItemViewCount(itemId, windowEnd, count));
        }
    }


    public static class ItemHotTopN extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
        ListState<ItemViewCount> itemViewCountListState;
        private int topN ;

        public ItemHotTopN(int topN) {
            this.topN = topN;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            itemViewCountListState = getRuntimeContext().getListState(new ListStateDescriptor<ItemViewCount>("itemViewCount", ItemViewCount.class));
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, Context ctx, Collector<String> out) throws Exception {
            itemViewCountListState.add(itemViewCount);
            ctx.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd() + 1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // ListState转为ArrayList
            ArrayList<ItemViewCount> arraylist = Lists.newArrayList(itemViewCountListState.get().iterator());

            arraylist.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().intValue() - o1.getCount().intValue();
                }
            });
            StringBuilder resultStringBuilder = new StringBuilder();
            resultStringBuilder.append("==================================="+"\n");
            resultStringBuilder.append("窗口结束时间：").append(new Timestamp(timestamp).toString()).append("\n");

            for (int i = 0; i < Math.min(topN, arraylist.size()); i++) {

                resultStringBuilder
                        .append("NO ")
                        .append(i + 1)
                        .append(": 商品ID = ")
                        .append(arraylist.get(i).getItemId())
                        .append(" 热门度 = ")
                        .append(arraylist.get(i).getCount())
                        .append("\n");

            }
            resultStringBuilder.append("===================================\n");
            out.collect(resultStringBuilder.toString());
            Thread.sleep(1000L);

        }

    }
}
