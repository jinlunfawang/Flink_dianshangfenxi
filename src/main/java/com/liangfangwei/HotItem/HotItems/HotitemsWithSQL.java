package com.liangfangwei.HotItem.HotItems;

import com.liangfangwei.HotItem.Bean.ItemBean;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author :LiangFangWei
 * @description: 使用FlinkSQL实现热门商品的统计
 * @date: 2021-12-16 15:29
 * <p>
 *-                   _ooOoo_
 *-                  o8888888o
 *-                  88" . "88
 *-                  (| -_- |)
 *-                   O\ = /O
 *-               ____/`---'\____
 *-             .   ' \\| |// `.
 *-              / \\||| : |||// \
 *-            / _||||| -:- |||||- \
 *-              | | \\\ - /// | |
 *-            | \_| ''\---/'' | |
 *-             \ .-\__ `-` ___/-. /
 *-          ___`. .' /--.--\ `. . __
 *-       ."" '< `.___\_<|>_/___.' >'"".
 *-      | | : `- \`.;`\ _ /`;.`/ - ` : | |
 *-        \ \ `-. \_ __\ /__ _/ .-` / /
 * ======`-.____`-.___\_____/___.-`____.-'======
 *                    `=---='
 * .............................................
 *          佛祖保佑             永无BUG
 */


public class HotitemsWithSQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setBufferTimeout(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 2. 读取数据，创建DataStream
        DataStream<String> inputStream = environment.readTextFile("/Users/liangfangwei/IdeaProjects/flinkUserAnalays/data_file/UserBehavior.csv");

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
        EnvironmentSettings build = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        // 1.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(environment, build);
        tableEnv.createTemporaryView("data_table", dataStream, "itemId, behavior, timestamp.rowtime as ts");

        Table resultSqlTable = tableEnv.sqlQuery("select * from " +
                "  ( select *, ROW_NUMBER() over (partition by windowEnd order by cnt desc) as row_num " +
                "  from ( " +
                "    select itemId, count(itemId) as cnt, HOP_END(ts, interval '5' minute, interval '1' hour) as windowEnd " +
                "    from data_table " +
                "    where behavior = 'pv' " +
                "    group by itemId, HOP(ts, interval '5' minute, interval '1' hour)" +
                "    )" +
                "  ) " +
                " where row_num <= 5 ");

        tableEnv.toRetractStream(resultSqlTable, Row.class).print();

        environment.execute("hot items with sql job");

    }
}
