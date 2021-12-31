import org.junit.Test;

import java.text.ParseException;

/**
 * Created by Liang on 2021/12/14.
 */
public class OtherTest {
    @Test
    public void test1() throws ParseException {


/*
        SimpleDateFormat simpleFormatter = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");

        long  timestamp = simpleFormatter.parse("17/05/2015:10:05:07").getTime();
        System.out.println(timestamp);*/

      /*  System.out.println("aello".charAt(0)+0);
        System.out.println(new Random(3).nextInt());
        System.out.println(System.currentTimeMillis());*/

    /*    Random random=new Random(100);
        while (true)
            System.out.println(random.nextInt(3));




    }*/
    Long a =((1511661601000L/24*60*60*1000L)+1)*24*60*60*1000L- 8*60*60*1000;
        System.out.println(a);

    }
}