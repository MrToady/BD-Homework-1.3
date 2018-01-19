import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
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

        log.debug("Combiner: Start iterate through values");
        for (HighBitOSWritable value : values) {
            log.debug("Combiner: merging values");
            valuesMap.merge(value.getOperationSystem(), 1, Integer::sum);
        }

        valuesMap.forEach((operationSystem, amount) -> {
            writable.setOperationSystem(operationSystem);
            writable.setHighBitPriceImpressions(amount);
            try {
                log.debug("{} - {} was written in the context", operationSystem, amount);
                context.write(key, writable);
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
