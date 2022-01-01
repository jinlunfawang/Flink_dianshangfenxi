package com.liangfangwei.adclick;

import com.liangfangwei.bean.AdOutputInfo;
import com.liangfangwei.bean.AdvertInfo;
import com.liangfangwei.bean.BlackAdUerInfo;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
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
 *
 * @author :LiangFangWei
 * @date: 2021-12-23 18:58
 * @description: 统计每个省份的每个广告的点击次数, 如果某个用户当天对广告的点击超过次数 输出作为一个流输出
 * <p>
 * 思路:
 * 1.最终输出形式
 * （省,窗口截止时间,总数）
 * 2. 创建增量聚合可以拿到总数 全窗口函数可以拿到窗口截止时间 和key
 * 3. 异常数据进行过滤,如果用户在某天对同一广告的点击次数如果超过一定次数 则单独作为流输出
 * 3.1 就要保存某个用户对某个广告的点击次数的状态,如果超过100次 并加入黑名单 如果在黑名单中直接返回 什么也不处理和统计
 * 3.2 如果没有超过 那么次数+1 输出数据
 */


public class AdStatisticsByProvince {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        executionEnvironment.setParallelism(1);
        DataStream<String> inputStream = executionEnvironment.readTextFile("/Users/liangfangwei/IdeaProjects/flinkUserAnalays/data_file/AdClickLog.csv");
        DataStream<AdvertInfo> processStream1 = inputStream.map(line -> {
            String[] split = line.split(",");
            return new AdvertInfo(split[0], split[1], split[2], Long.parseLong(split[4]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdvertInfo>() {
            @Override
            public long extractAscendingTimestamp(AdvertInfo element) {
                return element.getTimeStramp() * 1000L;
            }
        });
        // 过滤掉异常的流数据
        SingleOutputStreamOperator<AdvertInfo> fliterBlackStream = processStream1
                .keyBy("userId", "adId")
                .process(new BlackUserProcess(3));

        DataStream<AdOutputInfo> resultStream = fliterBlackStream
                .keyBy("province")
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new IncreaseAggreateEle(), new AllAggreateCount());
        fliterBlackStream.getSideOutput(new OutputTag<BlackAdUerInfo>("blacklist"){}).print("blacklist-user");

        resultStream.print("--->");

        executionEnvironment.execute();
    }


    /**
     * 过滤处异常数据
     */

    public static class BlackUserProcess extends KeyedProcessFunction<Tuple, AdvertInfo, AdvertInfo> {
        ValueState<Long> adClickCount;
        ValueState<Boolean> isBlackUser;


        private int bound;

        public BlackUserProcess(int bound) {

            this.bound = bound;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            adClickCount = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ad_click_count", Long.class, 0l));
            isBlackUser = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is_black_user", Boolean.class, false));


        }

        /**
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement(AdvertInfo value, Context ctx, Collector<AdvertInfo> out) throws Exception {
            // 1.判断是否到了设置的边界 注意状态只保留一天
            Long userIdClickCount = adClickCount.value();
            // 注册第二天的定时器 如果到了清楚状态
            Long timestamp = ctx.timerService().currentProcessingTime();
            Long clserTime = ((timestamp / 24 * 60 * 60 * 1000L) + 1) * 24 * 60 * 60 * 1000L - 8 * 60 * 60 * 1000;
            ctx.timerService().registerEventTimeTimer(clserTime);

            // 2.如果到了设置了边界
            if (userIdClickCount >= bound) {
                // 2.1 没有在黑名单中
                if (!isBlackUser.value()) {
                    // 加入黑名单 加入到侧输出流中
                    isBlackUser.update(true);
                    ctx.output(new OutputTag<BlackAdUerInfo>("blacklist") {
                               },
                            new BlackAdUerInfo(value.getUserId(), value.getAdId(), "click over " + userIdClickCount + "times."));
                }
                // 2.2 在黑名单 直接返回
                return;
            }


            // 3. 如果没有达到设置的边界 更新状态 输出该条数据
            adClickCount.update(userIdClickCount+1);
            out.collect(value);

        }


        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdvertInfo> out) throws Exception {
            adClickCount.clear();
            isBlackUser.clear();
        }
    }


    public static class IncreaseAggreateEle implements AggregateFunction<AdvertInfo, Long, Long> {


        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdvertInfo value, Long accumulator) {
            return accumulator+1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }


    public static class AllAggreateCount implements WindowFunction<Long, AdOutputInfo, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<AdOutputInfo> out) throws Exception {
            Timestamp formateDate = new Timestamp(window.getEnd());
            out.collect(new AdOutputInfo(tuple.getField(0).toString(),formateDate.toString(),input.iterator().next()));
        }
    }
}
