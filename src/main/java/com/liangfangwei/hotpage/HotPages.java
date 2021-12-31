package com.liangfangwei.hotpage;

import com.liangfangwei.bean.ApacheLogEvent;
import com.liangfangwei.bean.PageViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author :LiangFangWei
 * @description:
 * @date: 2021-12-16 19:48
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
 * `=---='
 * .............................................
 * 佛祖保佑             永无BUG
 * <p>
 * 需求
 * 每5分钟输出一次1小时之内排名前5的页面
 * 1 .小时统计一次结果 ,即开窗是一小时 收集1小时内的统计结果,按照窗口结束时间输出窗口内的结果。窗口的滑动步长设置为5min
 */


public class HotPages {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        executionEnvironment.setParallelism(1);

        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("/Users/liangfangwei/IdeaProjects/flinkUserAnalays/data_file/apache.log");

        SimpleDateFormat simpleFormatter = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");

        OutputTag<ApacheLogEvent> lateTag = new OutputTag<ApacheLogEvent>("late_date") {
        };

        DataStream<PageViewCount> streamPageViewCount = stringDataStreamSource.map(line -> {
            String[] s = line.split(" ");
            // 日期转时间戳
            Long timestamp = simpleFormatter.parse(s[3]).getTime();
            return new ApacheLogEvent(s[0], s[1], timestamp, s[5], s[6]);
        }).filter(date -> "GET".equals(date.getMethod()))
                .filter(data -> {
                    // 过滤处css  js png ico 结尾的
                    String regex = "((?!\\.(css|js|png|ico|jpg)$).)*$";
                    return Pattern.matches(regex, data.getUrl());
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(ApacheLogEvent apacheLogEvent) {

                        return apacheLogEvent.getTimestamp();
                    }
                }).keyBy("url")
                .timeWindow(Time.minutes(10), Time.seconds(5))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateTag)
                .aggregate(new HotPageIncreaseAgg(), new HotPageAllAgg());

        SingleOutputStreamOperator<String> windowEnd = streamPageViewCount
                .keyBy("windowEnd")
                .process(new MyProcessFunction(5));
        // 控制台输出
        windowEnd.print("data");
        windowEnd.getSideOutput(lateTag).print("late_date");
        executionEnvironment.execute();

    }


    public static class HotPageIncreaseAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {


        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    public static class HotPageAllAgg implements WindowFunction<Long, PageViewCount, Tuple, TimeWindow> {


        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {

            String url = tuple.getField(0);
            Long count = input.iterator().next();
            long windowEnd = window.getEnd();

            out.collect(new PageViewCount(url, windowEnd, count));
        }
    }

    public static class MyProcessFunction extends KeyedProcessFunction<Tuple, PageViewCount, String> {
        private Integer topSize;

        MapState<String, Long> hotPageCount;

        public MyProcessFunction(Integer topSize) {
            this.topSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {

              hotPageCount = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("hot_page_count", String.class, Long.class));
        }

        /**
         * 如果有迟到数据 需要覆盖就的数据
         * 那么定义一个map 加入相同的key 会被覆盖
         * 如果时间超过1分钟 那就清除状态
         */
        @Override
        public void processElement(PageViewCount pageViewCount, Context ctx, Collector<String> out) throws Exception {
            // map 类型 如果key相同就更新
                 hotPageCount.put(pageViewCount.getUrl(),pageViewCount.getCount());
                ctx.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd()+1);
        }


        /**
         * 输出list中的结果
         *
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws
                Exception {
            Long currentKey = ctx.getCurrentKey().getField(0);
            // 判断是否到了窗口关闭清理的时间, 如果是 直接清空状态
           if (timestamp == currentKey + 60 * 1000L) {
               hotPageCount.clear();
                return;
            }
            ArrayList<Map.Entry<String, Long>> pageViewCounts = Lists.newArrayList(hotPageCount.entries());

            pageViewCounts.sort(new Comparator<Map.Entry<String, Long>>() {
                @Override
                public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                    if(o1.getValue() > o2.getValue())
                        return -1;
                    else if(o1.getValue() < o2.getValue())
                        return 1;
                    else
                        return 0;
                }
            });
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.append("===================================\n");
            stringBuilder.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < Math.min(topSize, pageViewCounts.size()); i++) {
                Map.Entry<String, Long> stringLongEntry = pageViewCounts.get(i);
                stringBuilder.append("NO ").append(i + 1).append(":")
                        .append(" 页面URL = ").append(stringLongEntry.getKey())
                        .append(" 热门度 = ").append(stringLongEntry.getValue())
                        .append("\n");
            }
            stringBuilder.append("===============================\n\n");

            // 控制输出频率
           Thread.sleep(1000L);

            out.collect(stringBuilder.toString());

        }
    }


}
