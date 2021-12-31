package com.liangfangwei.appdownload;

import com.liangfangwei.bean.AnalysisBean;
import com.liangfangwei.bean.AppPromoto;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.Random;

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
 * @description:实时统计每小时各推广渠道的 点击 下载 安装数
 * --每隔5s输出一小时内的统计数据
 * @date: 2021-12-23 15:45
 * <p>
 * 需求
 * 1. 自定义数据源
 * 2. 开窗 窗口长度为1小时 滑动步长为5S
 * 3. 按照渠道+操作分组 每来一条数据窗口内聚合 再加上全窗口函数的时间 输出
 */


public class ChannenPromotionCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        executionEnvironment.setParallelism(1);

        DataStreamSource<AppPromoto> dataStreamSource = executionEnvironment.addSource(new MySourceFunction());


        DataStream<AnalysisBean> outPutStream = dataStreamSource.filter(appPromoto -> !"UNINSTALL".equals(appPromoto.getBehavior())).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AppPromoto>() {
            @Override
            public long extractAscendingTimestamp(AppPromoto element) {
                return element.getTimeStamp();
            }
        }).keyBy("channel", "behavior")
                .timeWindow(Time.hours(1), Time.seconds(3))
                .aggregate(new AppPromotoAgg(), new MarketingCountResult());

        outPutStream.print();
        executionEnvironment.execute();

    }


    public static class MySourceFunction implements SourceFunction<AppPromoto> {
        private boolean keepContinue = true;
        String[] channelArr = new String[]{"微博", "微信", "B站", "知乎", "贴吧", "抖音", "小红书"};
        String[] behaviorArr = new String[]{"CLIKC", "INSTALL", "REGISTER", "USING", "UNINSTALL", "SHARE"};

        @Override
        public void run(SourceContext<AppPromoto> ctx) throws Exception {
            Random random = new Random();
            while (keepContinue) {
                ctx.collect(new AppPromoto(channelArr[random.nextInt(channelArr.length)], System.currentTimeMillis(), behaviorArr[random.nextInt(behaviorArr.length)]));
                // Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            keepContinue = false;
        }
    }


    public static class AppPromotoAgg implements AggregateFunction<AppPromoto, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AppPromoto value, Long accumulator) {
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

    public static class MarketingCountResult extends ProcessWindowFunction<Long, AnalysisBean, Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, Context context, Iterable<Long> elements, Collector<AnalysisBean> out) throws Exception {
            Long count = elements.iterator().next();
            long endTime = context.window().getEnd();
            Timestamp timestamp = new Timestamp(endTime);
            out.collect(new AnalysisBean(timestamp.toString(), tuple.getField(0).toString(), tuple.getField(1).toString(), count));

        }


    }


}
