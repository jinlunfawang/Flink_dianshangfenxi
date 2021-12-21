package com.liangfangwei.HotItem.HotItems;

/**
 * @author :LiangFangWei
 * @description:
 * @date: 2021-12-16 16:58
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
 * -            佛祖保佑             永无BUG
 */

import com.liangfangwei.HotItem.Bean.ItemBean;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 使用SQL处理统计热门商品
 *
 * 1.统计每个窗口结束时的总数 和窗口结束时间
 * 2.根据窗口结束时间排序 输出 前5个的
 *
 */
public class HotItemsWithSQLPractise {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);
        DataStreamSource<String> inputStream = env.readTextFile("/Users/liangfangwei/IdeaProjects/flinkUserAnalays/data_file/UserBehavior.csv");
        SingleOutputStreamOperator<ItemBean> filterStream = inputStream
                .map(line -> {
                    String[] fields = line.split(",");
                    return new ItemBean(new Long(fields[0]), new Long(fields[1]), new Integer(fields[2]), fields[3], new Long(fields[4]));
                }).filter(data -> "pv".equals(data.getBehavior()));
        // 指定watermark类型 以及生成的时间
        DataStream<ItemBean> dataStream = filterStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ItemBean>() {
            @Override
            public long extractAscendingTimestamp(ItemBean element) {

                return element.getTimestamp()* 1000L;
            }
        });

        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(env, build);
        streamTableEnvironment.createTemporaryView("data_table",dataStream,"itemId,timestamp.rowtime as ts");
        String sql="select * from " +
                "  ( select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                "  from ( " +
                "    select itemId, count(itemId) as cnt, HOP_END(ts, interval '5' minute, interval '1' hour) as windowEnd " +
                "    from data_table " +
                "    group by itemId, HOP(ts, interval '5' minute, interval '1' hour)" +
                "    )" +
                "  ) " +
                " where row_num <= 5 ";
        Table table = streamTableEnvironment.sqlQuery(sql);

        streamTableEnvironment.toRetractStream(table, Row.class).print();

        env.execute("hot items2 with sql job");
    }
}
