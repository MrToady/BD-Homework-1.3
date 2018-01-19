import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

@Slf4j
public class HighBitPriceMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
    private static final String ENTRY_DELIMITER = "\n";
    private static final String COLUMN_DELIMITER = "\t";
    private static final IntWritable ONE = new IntWritable(1);
    private IntWritable cityId = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), ENTRY_DELIMITER);

        while (tokenizer.hasMoreTokens()) {
            String[] dataArray = tokenizer.nextToken().split(COLUMN_DELIMITER);

            int cityIdInt = Integer.valueOf(dataArray[7]);
            int bidPrice = Integer.valueOf(dataArray[19]);

            if (bidPrice > 250) {
                cityId.set(cityIdInt);
                context.write(cityId, ONE);
            }
        }
    }
}
