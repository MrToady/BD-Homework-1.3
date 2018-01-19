import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class HighBitPriceMapperTest {

    public static final String LOG_STRING_1 = "2e72d1bd7185fb76d69c852c57436d37\t20131019025500549\t1\tCAD06D3WCtf\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.117.187.*\t216\t234\t2\t33235ca84c5fee9254e6512a41b3ad5e\t8bbb5a81cc3d680dd0c27cf4886ddeae\tnull\t3061584349\t728\t90\tOtherView\tNa\t5\t7330\t277\t48\tnull\t2259\t10057,13800,13496,10079,10076,10075,10093,10129,10024,10006,10110,13776,10146,10120,10115,10063";
    public static final String LOG_STRING_2 = "93074d8125fa8945c5a971c2374e55a8\t20131019161502142\t1\tCAH9FYCtgQf\tMozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)\t119.145.140.*\t216\t222\t1\t20fc675468712705dbf5d3eda94126da\t9c1ecbb8a301d89a8d85436ebf393f7f\tnull\tmm_10982364_973726_8930541\t300\t250\tFourthView\tNa\t0\t7323\t294\t201\tnull\t2259\t10057,10059,10083,10102,10024,10006,10110,10031,10063,10116";
    public static final String LOG_STRING_3 = "d503d08833987e6c792dc45ea1810800\t20131019161905104\t1\tCATFtG0VfMY\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17 SE 2.X MetaSr 1.0\t183.19.52.*\t216\t226\t2\t13625cb070ffb306b425cd803c4b7ab4\tdf6e51cda096af59f06fe10e5d2285f6\tnull\t350639023\t300\t250\tOtherView\tNa\t89\t7323\t277\t260\tnull\t2259\t10057,10048,10059,10079,10076,10077,10083,13866,10006,10024,10148,13776,10111,10063,10116";
    public static final String LOG_STRING_4 = "710f5852a9bec40561ea85d7ff51a4e6\t20131019161902429\t1\tCATFtG0VfMY\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17 SE 2.X MetaSr 1.0\t183.19.52.*\t216\t226\t2\t13625cb070ffb306b425cd803c4b7ab4\t733d4fa04458005aacd0e3689639fdc5\tnull\t3195670606\t728\t90\tOtherView\tNa\t52\t7330\t277\t52\tnull\t2259\t10057,10048,10059,10079,10076,10077,10083,13866,10006,10024,10148,13776,10111,10063,10116";
    public static final String LOG_STRING_5 = "b4791d1310887ab13162503d51696ad3\t20131019161905089\t1\tCATFtG0VfMY\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17 SE 2.X MetaSr 1.0\t183.19.52.*\t216\t226\t2\t13625cb070ffb306b425cd803c4b7ab4\t733d4fa04458005aacd0e3689639fdc5\tnull\t3533117577\t300\t250\tOtherView\tNa\t52\t7323\t277\t260\tnull\t2259\t10057,10048,10059,10079,10076,10077,10083,13866,10006,10024,10148,13776,10111,10063,10116";
    public static final String LOG_LOW_PRICE = "b4791d1310887ab13162503d51696ad3\t20131019161905089\t1\tCATFtG0VfMY\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17 SE 2.X MetaSr 1.0\t183.19.52.*\t216\t226\t2\t13625cb070ffb306b425cd803c4b7ab4\t733d4fa04458005aacd0e3689639fdc5\tnull\t3533117577\t300\t250\tOtherView\tNa\t52\t7323\t240\t260\tnull\t2259\t10057,10048,10059,10079,10076,10077,10083,13866,10006,10024,10148,13776,10111,10063,10116";
    public static final Text INPUT_1 = new Text(LOG_STRING_1 + "\n" + LOG_STRING_2);
    public static final Text INPUT_2 = new Text(LOG_STRING_3 + "\n" + LOG_STRING_4 + "\n" + LOG_STRING_5);
    public static final Text INPUT_3 = new Text(LOG_LOW_PRICE);
    public static final HighBitOSWritable ONE = new HighBitOSWritable(1, "Windows XP");
    public static final IntWritable OUTPUT_KEY_1 = new IntWritable(234);
    public static final IntWritable OUTPUT_KEY_2 = new IntWritable(222);
    public static final IntWritable OUTPUT_KEY_3 = new IntWritable(226);

    private MapDriver<LongWritable, Text, IntWritable, HighBitOSWritable> mapDriver;

    @Before
    public void setUp() {
        Mapper mapper = new HighBitPriceMapper();
        mapDriver = new MapDriver<>();
        mapDriver.setMapper(mapper);
    }

    @Test
    public void testIfMapperParsesLosStringProperly() throws IOException {
        mapDriver.withInput(new LongWritable(), INPUT_1);
        mapDriver.withOutput(OUTPUT_KEY_1, ONE);
        mapDriver.withOutput(OUTPUT_KEY_2, ONE);
        mapDriver.runTest();
    }

    @Test
    public void testIfMapperParsesLosStringProperly2() throws IOException {
        mapDriver.withInput(new LongWritable(), INPUT_2);
        mapDriver.withOutput(OUTPUT_KEY_3, ONE);
        mapDriver.withOutput(OUTPUT_KEY_3, ONE);
        mapDriver.withOutput(OUTPUT_KEY_3, ONE);
        mapDriver.runTest();
    }

    @Test
    public void testIfMapperCutOffLowPrice() throws IOException {
        mapDriver.withInput(new LongWritable(), INPUT_3);
        mapDriver.runTest();
    }
}