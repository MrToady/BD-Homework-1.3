import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class HighBitPriceReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
    private IntWritable sum = new IntWritable();
    private Map<Integer, Text> map = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("city.en.txt")));
        String line;
        while ((line = reader.readLine()) != null) {
            String[] idAndCityName = line.split("[\t ]");
            int id = Integer.valueOf(idAndCityName[0]);
            String cityName = idAndCityName[1];
            map.put(id, new Text(cityName));
        }
    }

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int highBitPriceImpressionsCounter = 0;

        for (IntWritable value : values) {
            highBitPriceImpressionsCounter += value.get();
        }

        Text cityTextValue = map.getOrDefault(key.get(), new Text(String.format("Unknown ID:%d", key.get())));
        sum.set(highBitPriceImpressionsCounter);
        context.write(cityTextValue, sum);
    }
}
