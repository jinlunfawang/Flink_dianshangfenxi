package com.liangfangwei.ordercheck;

import com.liangfangwei.bean.OrderTimeoutInfo;
import com.liangfangwei.bean.OrderInfo;
import org.apache.flink.cep.*;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

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
 * @description:检测15分钟内没有支付的订单
 * @date: 2021-12-27 22:23
 */


public class OrderCheck {
    private static final Logger logger = LoggerFactory.getLogger(OrderCheck.class);

    public static void main(String[] args) throws Exception {
           // 1.
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("/Users/liangfangwei/IdeaProjects/flinkUserAnalays/data_file/OrderLog.csv");
        SingleOutputStreamOperator<OrderInfo> objectSingleOutputStreamOperator = stringDataStreamSource.map(line -> {
            String[] split = line.split(",");
            return new OrderInfo(split[0], split[1], split[2], Long.parseLong(split[3]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderInfo>() {

            @Override
            public long extractAscendingTimestamp(OrderInfo element) {
                return element.getTimeStamp()*1000L;
            }
        });


        // 2  定义规则

        Pattern<OrderInfo, OrderInfo> orderPayPattern = Pattern.<OrderInfo>begin("create").where(new SimpleCondition<OrderInfo>() {
            @Override
            public boolean filter(OrderInfo value) throws Exception {
                return "create".equals(value.getStatus());
            }
        }).followedBy("pay").where(new SimpleCondition<OrderInfo>() {

            @Override
            public boolean filter(OrderInfo value) throws Exception {
                return "pay".equals(value.getStatus());
            }
    }).within(Time.minutes(15));
        PatternStream<OrderInfo> orderStream = CEP.pattern(objectSingleOutputStreamOperator.keyBy("orderId"), orderPayPattern);
        OutputTag<OrderTimeoutInfo> outputTag = new OutputTag<OrderTimeoutInfo>("timeoutStream") {
        };
        SingleOutputStreamOperator<OrderTimeoutInfo> resultStream = orderStream.select(outputTag, new OrderTimeoutSelect(), new OrderPaySelect());
        resultStream.print("payed normally");
        resultStream.getSideOutput(outputTag).print("timeout");

        executionEnvironment.execute("order timeout detect job");
    }

    /**
     * 什么时候 会判断超时：定义的时间范围内如果没有匹配上或者 就判断超时
     * 输出到哪里：超时事件会输出到侧输出流中
     */
    public static class OrderTimeoutSelect implements PatternTimeoutFunction<OrderInfo,OrderTimeoutInfo>{
    @Override
    public OrderTimeoutInfo timeout(Map<String, List<OrderInfo>> pattern, long timeoutTimestamp) throws Exception {
        logger.error("rrrr_locker: get locker fail: key={}", pattern.toString());
        OrderInfo OrderInfo = pattern.get("create").get(0);

        return new OrderTimeoutInfo(OrderInfo.getOrderId(),"timeout"+timeoutTimestamp);
    }



    }

    public  static  class  OrderPaySelect implements PatternSelectFunction<OrderInfo, OrderTimeoutInfo>{
        @Override
        public OrderTimeoutInfo select(Map<String, List<OrderInfo>> pattern) throws Exception {
            OrderInfo OrderInfo = pattern.get("pay").get(0);

            return new OrderTimeoutInfo(OrderInfo.getOrderId(),"pay");
        }
    }

}
