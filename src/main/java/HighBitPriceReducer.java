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
public class HighBitPriceReducer extends Reducer<HighBitOSWritable, IntWritable, Text, IntWritable> {
    private IntWritable sum = new IntWritable();
    private Map<Integer, Text> cityMap = new HashMap<>();
    private int cityIDint = -1;
    private int globalImpressionsCounter = 0;

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
    protected void reduce(HighBitOSWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int highBitPriceImpressionsCounter = 0;
        for (IntWritable value : values) {
            highBitPriceImpressionsCounter += value.get();
        }

        if (key.getCityID() == cityIDint) {
            globalImpressionsCounter += highBitPriceImpressionsCounter;
        } else {
            writePreviousPairIntoContext(context);
            globalImpressionsCounter = highBitPriceImpressionsCounter;
            cityIDint = key.getCityID();

        }
    }

    private void writePreviousPairIntoContext(Context context) throws IOException, InterruptedException {
        if (cityIDint != -1) {
            Text cityTextValue = cityMap.getOrDefault(cityIDint, new Text(String.format("Unknown ID:%d", cityIDint)));
            sum.set(globalImpressionsCounter);
            context.write(cityTextValue, sum);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        writePreviousPairIntoContext(context);
    }
}
