import com.liangfangwei.HotItem.Bean.PageViewCount;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.junit.Test;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created by Liang on 2021/12/14.
 */
public class OtherTest {
    @Test
    public void test1() throws ParseException {



        SimpleDateFormat simpleFormatter = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");

        long  timestamp = simpleFormatter.parse("17/05/2015:10:05:07").getTime();
        System.out.println(timestamp);

    }


}
