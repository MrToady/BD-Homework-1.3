import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class HighBitPriceReducer extends Reducer<IntWritable, HighBitOSWritable, Text, IntWritable> {
    private IntWritable sum = new IntWritable();
    private Map<Integer, Text> cityMap = new HashMap<>();

    /**
     * Summarize number of high bit price impressions from values
     * Replace city ID to string representation of a city from cityMap and writes into context
     * Writes constructed writable into context
     *
     * @see {@link HighBitOSWritable}
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        log.debug("start Setup");
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("city.en.txt")));
//        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("/home/taras/Desktop/hadoop_distr/Homework1-3 dataset/city.en.txt")));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] idAndCityName = line.split("[\t ]");
            int id = Integer.valueOf(idAndCityName[0]);
            String cityName = idAndCityName[1];
            cityMap.put(id, new Text(cityName));
        }
        log.debug("city map created");

    }

    @Override
    protected void reduce(IntWritable key, Iterable<HighBitOSWritable> values, Context context) throws IOException, InterruptedException {
        log.debug("reduce method start");
        int highBitPriceImpressionsCounter = 0;

        for (HighBitOSWritable value : values) {
            log.debug("summarizing HighBitPriceImpressions current sum:{}", highBitPriceImpressionsCounter);
            highBitPriceImpressionsCounter += value.getHighBitPriceImpressions();
        }

        Text cityTextValue = cityMap.getOrDefault(key.get(), new Text(String.format("Unknown ID:%d", key.get())));
        sum.set(highBitPriceImpressionsCounter);

        log.debug("write in context");
        context.write(cityTextValue, sum);
        log.debug("{} - {} written in context", cityTextValue.toString(), highBitPriceImpressionsCounter);
    }
}
