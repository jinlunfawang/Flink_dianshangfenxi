import org.junit.Test;

import java.sql.Timestamp;

/**
 * Created by Liang on 2021/12/14.
 */
public class OtherTest {
    @Test
    public void test1(){

        Timestamp timestamp = new Timestamp(1511658000 - 1);
        System.out.println(timestamp);

    }
}
