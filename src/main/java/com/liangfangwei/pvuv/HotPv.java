package com.liangfangwei.pvuv;

import com.liangfangwei.bean.ItemBean;
import com.liangfangwei.bean.PageViewCount;
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

import java.util.Random;

/**
 * @author :LiangFangWei
 * @date: 2021-12-18 21:01
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
 * @description: 统计每个小时内的pv总数
 * 1.输出一个小时内的统计结果,不能全窗口聚合,如果是全窗口聚合 那么所有的都在一个分区中会数据倾斜
 * 2.先给每条数据设置key,根据key分散 预聚合 最终再聚合
 */


public class HotPv {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStream<String> inputPath = env.readTextFile("/Users/liangfangwei/IdeaProjects/flinkUserAnalays/data_file/UserBehavior.csv");

        //1.转为POJO 分配时间戳
        DataStream<ItemBean> dataStream = inputPath.map(line -> {
            String[] split = line.split(",");
            return new ItemBean(new Long(split[0]), new Long(split[1]), new Integer(split[2]), split[3], new Long(split[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ItemBean>() {
            @Override
            public long extractAscendingTimestamp(ItemBean itemBean) {
                return itemBean.getTimestamp() * 1000L;
            }
        });

        SingleOutputStreamOperator<PageViewCount> dataStream2 = dataStream.filter(data -> "pv".equals(data.getBehavior()))
                .map(new MapFunction<ItemBean, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(ItemBean value) throws Exception {
                        Random random = new Random();
                        return new Tuple2<>(random.nextInt(10), 1L);
                    }
                })
                .keyBy(data -> data.f0)
                .timeWindow(Time.hours(1))
                .aggregate(new MyAggreatePVCount(), new MyAllWindowsPVView());

        SingleOutputStreamOperator<PageViewCount> widnowEnd = dataStream2
                .keyBy(PageViewCount::getWindowEnd)
                .process(new MyValueProcess());
        widnowEnd.print();
        env.execute("2");
    }

    public static class MyAggreatePVCount implements AggregateFunction<Tuple2<Integer, Long>, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Integer, Long> value, Long accumulator) {
            return accumulator + 1L;
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

    public static class MyAllWindowsPVView implements WindowFunction<Long, PageViewCount, Integer, TimeWindow> {

        @Override
        public void apply(Integer integer, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {

            out.collect( new PageViewCount(integer.toString(), window.getEnd(), input.iterator().next()) );

        }
    }

    public static class MyValueProcess extends KeyedProcessFunction<Long, PageViewCount, PageViewCount> {
        ValueState<Long> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
           valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("pv_value_state", Long.class,0L));


        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
           valueState.update( valueState.value() + value.getCount() );

            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {

            // 定时器触发，所有分组count值都到齐，直接输出当前的总count数量
             long  totalCount = valueState.value();
            out.collect(new PageViewCount("pv", ctx.getCurrentKey(), totalCount));
            // 清空状态
            valueState.clear();
        }
    }



}
