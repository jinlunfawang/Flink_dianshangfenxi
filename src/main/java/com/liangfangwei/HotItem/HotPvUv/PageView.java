package com.liangfangwei.HotItem.HotPvUv;

import com.liangfangwei.HotItem.Bean.ItemBean;
import com.liangfangwei.HotItem.Bean.PageViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
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

import java.net.URL;
import java.util.Random;

/**
 * @author :LiangFangWei
 * @description:
 * @date: 2021-12-19 16:08
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
 */




    public class PageView {
        public static void main(String[] args) throws Exception{
            // 1. 创建执行环境
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

            // 2. 读取数据，创建DataStream
            DataStream<String> inputStream = env.readTextFile("/Users/liangfangwei/IdeaProjects/flinkUserAnalays/data_file/UserBehavior.csv");

            // 3. 转换为POJO，分配时间戳和watermark
            DataStream<ItemBean> dataStream = inputStream
                    .map(line -> {
                        String[] fields = line.split(",");
                        return new ItemBean(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                    })
                    .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ItemBean>() {
                        @Override
                        public long extractAscendingTimestamp(ItemBean element) {
                            return element.getTimestamp() * 1000L;
                        }
                    });

            //  并行任务改进，设计随机key，解决数据倾斜问题
            SingleOutputStreamOperator<PageViewCount> pvStream = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                    .map(new MapFunction<ItemBean, Tuple2<Integer, Long>>() {
                        @Override
                        public Tuple2<Integer, Long> map(ItemBean value) throws Exception {
                            Random random = new Random();
                            return new Tuple2<>(random.nextInt(10), 1L);
                        }
                    })
                    .keyBy(data -> data.f0)
                    .timeWindow(Time.hours(1))
                    .aggregate(new PvCountAgg(), new PvCountResult());

            // 将各分区数据汇总起来
            DataStream<PageViewCount> pvResultStream = pvStream
                    .keyBy(PageViewCount::getWindowEnd)
                    .process(new TotalPvCount());

            pvResultStream.print();

            env.execute("pv count job");
        }

        // 实现自定义预聚合函数
        public static class PvCountAgg implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {
            @Override
            public Long createAccumulator() {
                return 0L;
            }

            @Override
            public Long add(Tuple2<Integer, Long> value, Long accumulator) {
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

        // 实现自定义窗口
        public static class PvCountResult implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {
            @Override
            public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
                out.collect( new PageViewCount(integer.toString(), window.getEnd(), input.iterator().next()) );
            }
        }

        // 实现自定义处理函数，把相同窗口分组统计的count值叠加
        public static class TotalPvCount extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {
            // 定义状态，保存当前的总count值
            ValueState<Long> totalCountState;

            @Override
            public void open(Configuration parameters) throws Exception {
                totalCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("total-count", Long.class, 0L));
            }

            @Override
            public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
                totalCountState.update( totalCountState.value() + value.getCount() );
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
                // 定时器触发，所有分组count值都到齐，直接输出当前的总count数量
                Long totalCount = totalCountState.value();
                out.collect(new PageViewCount("pv", ctx.getCurrentKey(), totalCount));
                // 清空状态
                totalCountState.clear();
            }
        }
    }

