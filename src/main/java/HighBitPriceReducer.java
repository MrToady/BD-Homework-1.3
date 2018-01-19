import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

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
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("city.en.txt")));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] idAndCityName = line.split("[\t ]");
            int id = Integer.valueOf(idAndCityName[0]);
            String cityName = idAndCityName[1];
            cityMap.put(id, new Text(cityName));
        }
    }

    @Override
    protected void reduce(IntWritable key, Iterable<HighBitOSWritable> values, Context context) throws IOException, InterruptedException {
        int highBitPriceImpressionsCounter = 0;

        for (HighBitOSWritable value : values) {
            highBitPriceImpressionsCounter += value.getHighBitPriceImpressions();
        }

        Text cityTextValue = cityMap.getOrDefault(key.get(), new Text(String.format("Unknown ID:%d", key.get())));
        sum.set(highBitPriceImpressionsCounter);
        context.write(cityTextValue, sum);
    }
}
