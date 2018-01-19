import lombok.extern.slf4j.Slf4j;
import net.sf.uadetector.UserAgentStringParser;
import net.sf.uadetector.service.UADetectorServiceFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

@Slf4j
public class HighBitPriceMapper extends Mapper<LongWritable, Text, IntWritable, HighBitOSWritable> {
    private static final String ENTRY_DELIMITER = "\n";
    private static final String COLUMN_DELIMITER = "\t";
    private final UserAgentStringParser userAgentStringParser = UADetectorServiceFactory.getResourceModuleParser();
    private HighBitOSWritable writable = new HighBitOSWritable();
    private IntWritable cityId = new IntWritable();

    /**
     * Splits input text in strings
     * Splits the string in String array
     * Parses information about city ID, price and User Agent from corresponding elements of array
     * Constructs writable and writes it into context
     *
     * @see {@link HighBitOSWritable}
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokenizer = new StringTokenizer(value.toString(), ENTRY_DELIMITER);

        while (tokenizer.hasMoreTokens()) {
            String[] dataArray = tokenizer.nextToken().split(COLUMN_DELIMITER);

            int cityIdInt = Integer.valueOf(dataArray[7]);
            int bidPrice = Integer.valueOf(dataArray[19]);
            String userAgent = dataArray[4];

            if (bidPrice > 250) {
                cityId.set(cityIdInt);
                writable.setHighBitPriceImpressions(1);
                writable.setOperationSystem(getOperationSystemName(userAgent));
                context.write(cityId, writable);
            }
        }
    }

    private String getOperationSystemName(String userAgent) {
        return userAgentStringParser.parse(userAgent).getOperatingSystem().getName();
    }
}
