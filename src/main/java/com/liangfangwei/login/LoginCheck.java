package com.liangfangwei.login;

import com.liangfangwei.bean.LoginFailInfo;
import com.liangfangwei.bean.LoginInfo;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
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
 * @date: 2021-12-26 17:11
 * @description: 连续登陆失败检测 输出两秒内登陆失败两次的记录
 */


public class LoginCheck {

    public static void main(String[] args) throws Exception {

        // 1.定义环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        DataStreamSource<String> stringDataStreamSource = executionEnvironment.readTextFile("/Users/liangfangwei/IdeaProjects/flinkUserAnalays/data_file/LoginLog.csv");

        // 2.包装为对象

        KeyedStream<LoginInfo, Tuple> keyedStream = stringDataStreamSource.map(line -> {
            String[] split = line.split(",");
            return new LoginInfo(split[0], split[2], Long.parseLong(split[3]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LoginInfo>(Time.seconds(3)) {

            @Override
            public long extractTimestamp(LoginInfo element) {
                return element.getTimeStamp() * 1000L;
            }
        }).keyBy("status");


        // 3.定义规则
        // 3.1 创建规则
        Pattern<LoginInfo, LoginInfo> failPattern = Pattern.<LoginInfo>begin("loginFailEvent").where(new SimpleCondition<LoginInfo>() {
            @Override
            public boolean filter(LoginInfo value) throws Exception {
                return "fail".equals(value.getStatus());
            }
            // 连续三次登陆失败  consecutive 设置为严格近邻居
        }).times(3).consecutive().within(Time.seconds(5));

        // 3.2 规则匹配到流上

        PatternStream<LoginInfo> pattern = CEP.pattern(keyedStream, failPattern);
        // 3.3 筛选数据
        SingleOutputStreamOperator selectStream = pattern.select(new PatternSelectFunction<LoginInfo, LoginFailInfo>() {

            @Override
            public LoginFailInfo select(Map<String, List<LoginInfo>> pattern) throws Exception {
                List<LoginInfo> loginFailEvent = pattern.get("loginFailEvent");
                LoginInfo firstFail = loginFailEvent.get(0);
                String userId = firstFail.getUserId();
                LoginInfo lastFail = pattern.get("loginFailEvent").get(loginFailEvent.size()-1);
                Timestamp firstFailTimeStamp = new Timestamp(firstFail.getTimeStamp() * 1000L);
                Timestamp secondFailTimeStamp = new Timestamp(lastFail.getTimeStamp() * 1000L);
                return new LoginFailInfo(userId, firstFailTimeStamp.toString(), secondFailTimeStamp.toString(), "连续"+loginFailEvent.size()+"登陆失败");

            }
        });
        selectStream.print();
        executionEnvironment.execute();

    }


}
