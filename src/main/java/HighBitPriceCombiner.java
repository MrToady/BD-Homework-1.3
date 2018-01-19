import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class HighBitPriceCombiner extends Reducer<IntWritable, HighBitOSWritable, IntWritable, HighBitOSWritable> {
    private HighBitOSWritable writable = new HighBitOSWritable();

    /**
     * Creates a map of OSN (here and next "Operation System Name") values and number of impressions
     * Summarize numbers if the OSN is the same
     * Writes constructed writable into context
     *
     * @see {@link HighBitOSWritable}
     */
    @Override
    protected void reduce(IntWritable key, Iterable<HighBitOSWritable> values, Context context) {
        Map<String, Integer> valuesMap = new HashMap<>();

        for (HighBitOSWritable value : values) {
            valuesMap.merge(value.getOperationSystem(), 1, Integer::sum);
        }

        valuesMap.forEach((operationSystem, amount) -> {
            writable.setOperationSystem(operationSystem);
            writable.setHighBitPriceImpressions(amount);
            try {
                context.write(key, writable);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
