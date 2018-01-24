import eu.bitwalker.useragentutils.UserAgent;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

@Slf4j
public class HighBitPriceMapper extends Mapper<LongWritable, Text, HighBitOSWritable, IntWritable> {
    private static final String COLUMN_DELIMITER = "\t";
    private UserAgent userAgent;
    private HighBitOSWritable outputKey = new HighBitOSWritable();
    private IntWritable one = new IntWritable(1);

    /**
     * Splits input string in String array
     * Parses information about city ID, price and User Agent from corresponding elements of array
     * Constructs writable and writes it into context
     *
     * @see {@link HighBitOSWritable}
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        log.debug("splitting log string");
        String[] dataArray = value.toString().split(COLUMN_DELIMITER);
        int bidPrice = Integer.valueOf(dataArray[19]);

        if (bidPrice > 250) {
            int cityIdInt = Integer.valueOf(dataArray[7]);
            String userAgentString = dataArray[4];

            outputKey.setCityID(cityIdInt);
            outputKey.setOperationSystem(getOperationSystemName(userAgentString));

            log.debug("Mapper: write in context: {} - {}", outputKey, one);
            context.write(outputKey, one);
        }
    }

    private String getOperationSystemName(String userAgentString) {
        userAgent = new UserAgent(userAgentString);
        return userAgent.getOperatingSystem().getName();
    }
}
