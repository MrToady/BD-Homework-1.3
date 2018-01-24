import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

@Slf4j
public class HighBitPriceCombiner extends Reducer<HighBitOSWritable, IntWritable, HighBitOSWritable, IntWritable> {
    private IntWritable value = new IntWritable();

    /**
     * Summarize values of amounts by keys
     * Writes constructed writable into context
     *
     * @see {@link HighBitOSWritable}
     */
    @Override
    protected void reduce(HighBitOSWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int valueCounter = 0;
        log.debug("Combiner: Start iterate through values");
        for (IntWritable ignored : values) {
            valueCounter++;
        }
        value.set(valueCounter);
        context.write(key, value);
    }
}
