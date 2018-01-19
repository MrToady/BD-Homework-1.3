import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HighBitPriceCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    private IntWritable sum = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int highBitPriceImpressionsCounter = 0;

        for (IntWritable ignored : values) {
            highBitPriceImpressionsCounter++;
        }
        sum.set(highBitPriceImpressionsCounter);
        context.write(key, sum);
    }
}
