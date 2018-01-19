import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HighBitPriceReducerTest {
    private ReduceDriver<IntWritable, HighBitOSWritable, Text, IntWritable> reduceDriver;
    private List<HighBitOSWritable> valuesList;
    private static final IntWritable INPUT_KEY_1 = new IntWritable(41);
    //This ID is absent in CityID map
    private static final IntWritable INPUT_KEY_2 = new IntWritable(40);
    private static final HighBitOSWritable INPUT_VALUE_1 = new HighBitOSWritable(1, "Windows XP");
    private static final HighBitOSWritable INPUT_VALUE_2 = new HighBitOSWritable(2, "Windows XP");
    private static final HighBitOSWritable INPUT_VALUE_3 = new HighBitOSWritable(3, "Windows XP");
    private static final HighBitOSWritable INPUT_VALUE_4 = new HighBitOSWritable(4, "Windows XP");
    private static final HighBitOSWritable INPUT_VALUE_5 = new HighBitOSWritable(5, "Windows XP");

    private static final Text OUTPUT_KEY_1 = new Text("shenyang");
    private static final Text OUTPUT_KEY_2 = new Text("Unknown ID:40");
    private static final IntWritable OUTPUT_VALUE = new IntWritable(15);

    @Before
    public void setUp() {
        Reducer reducer = new HighBitPriceReducer();
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);

        valuesList = new ArrayList<>();
        valuesList.add(INPUT_VALUE_1);
        valuesList.add(INPUT_VALUE_2);
        valuesList.add(INPUT_VALUE_3);
        valuesList.add(INPUT_VALUE_4);
        valuesList.add(INPUT_VALUE_5);
    }

    @Test
    public void testIfReducerProcessValuesProperly() throws IOException {
        reduceDriver.setInput(INPUT_KEY_1, valuesList);
        reduceDriver.withOutput(OUTPUT_KEY_1, OUTPUT_VALUE);
        reduceDriver.runTest();
    }

    @Test
    public void testIfReducerProcessValuesProperlyIfCityAbsent() throws IOException {
        reduceDriver.setInput(INPUT_KEY_2, valuesList);
        reduceDriver.withOutput(OUTPUT_KEY_2, OUTPUT_VALUE);
        reduceDriver.runTest();
    }
}