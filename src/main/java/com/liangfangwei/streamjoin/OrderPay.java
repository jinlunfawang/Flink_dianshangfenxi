package com.liangfangwei.streamjoin;

import com.liangfangwei.bean.OrderInfo;
import com.liangfangwei.bean.Receipt;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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
 * @description: 检测订单是否是否到账
 * @date: 2021-12-30 19:23
 */


public class OrderPay {
    private final static OutputTag<OrderInfo> unmatchedPays = new OutputTag<OrderInfo>("unmatchedPays") {
    };


    private final static OutputTag<Receipt> unmatchedReceipts = new OutputTag<Receipt>("unmatchedReceipts") {
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //1. 支付数据
        DataStreamSource<String> inputSteam1 = env.readTextFile("/Users/liangfangwei/IdeaProjects/flinkUserAnalays/data_file/OrderLog.csv");
        SingleOutputStreamOperator<OrderInfo> orderStream = inputSteam1.map(line -> {
            String[] split = line.split(",");
            return new OrderInfo(split[0], split[1], split[2], Long.parseLong(split[3]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderInfo>() {

            @Override
            public long extractAscendingTimestamp(OrderInfo element) {
                return element.getTimeStamp() * 1000L;
            }
        });
        // 2.入账数据
        DataStreamSource<String> inputStream2 = env.readTextFile("/Users/liangfangwei/IdeaProjects/flinkUserAnalays/data_file/OrderLog.csv");
        SingleOutputStreamOperator<Receipt> payStream = inputStream2.map(line -> {
            String[] split = line.split(",");
            return new Receipt(split[0], split[1], Long.parseLong(split[2]));
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Receipt>() {

            @Override
            public long extractAscendingTimestamp(Receipt element) {
                return element.getTimeStamp() * 1000L;
            }
        });
        // 3.双里join
        SingleOutputStreamOperator<Tuple2<OrderInfo, Receipt>> resultStream = orderStream.keyBy("payId").connect(payStream.keyBy("payId")).process(new DoubleStreamJoinProcess());


        // 4.如果join上返回
        resultStream.print("matched");
        resultStream.getSideOutput(unmatchedPays).print("unmatchedPays");
    }


    public static class DoubleStreamJoinProcess extends CoProcessFunction<OrderInfo, Receipt, Tuple2<OrderInfo, Receipt>> {

        ValueState<OrderInfo> payState;
        ValueState<Receipt> receiptState;

        @Override
        public void open(Configuration parameters) throws Exception {
            payState = getRuntimeContext().getState(new ValueStateDescriptor<OrderInfo>("pay", OrderInfo.class));
            receiptState = getRuntimeContext().getState(new ValueStateDescriptor<Receipt>("receipt", Receipt.class));

        }


        @Override
        public void processElement1(OrderInfo orderInfo, Context ctx, Collector<Tuple2<OrderInfo, Receipt>> out) throws Exception {

            Receipt receipt = receiptState.value();
            //  取出流2
            if (receipt != null) {
                out.collect(new Tuple2<>(orderInfo, receipt));
                receiptState.clear();
            } else {
                payState.update(orderInfo);
                ctx.timerService().registerEventTimeTimer(orderInfo.getTimeStamp() * 1000L + 5000L);

            }
        }

        @Override
        public void processElement2(Receipt receipt, Context ctx, Collector<Tuple2<OrderInfo, Receipt>> out) throws Exception {


            // 取出流1
            OrderInfo orderInfo = payState.value();
            if (orderInfo != null) {
                out.collect(new Tuple2<>(orderInfo, receipt));
                payState.clear();
            } else {
                receiptState.update(receipt);
                ctx.timerService().registerEventTimeTimer(receipt.getTimeStamp() * 1000L + 5000L);


            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderInfo, Receipt>> out) throws Exception {
            if (payState.value() != null) {
                ctx.output(unmatchedPays, payState.value());
            }
            if (receiptState.value() != null) {
                ctx.output(unmatchedReceipts, receiptState.value());
            }
            payState.clear();
            receiptState.clear();

            super.onTimer(timestamp, ctx, out);
        }
    }

}
