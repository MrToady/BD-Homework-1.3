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
    private HighBitOSWritable writable;
    private IntWritable cityId = new IntWritable();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        log.debug("mapper setup");
        writable = new HighBitOSWritable();
        writable.setHighBitPriceImpressions(1);
    }

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

        String[] dataArray;
        while (tokenizer.hasMoreTokens()) {
            log.debug("splitting log string");
            dataArray = tokenizer.nextToken().split(COLUMN_DELIMITER);
            int bidPrice = Integer.valueOf(dataArray[19]);

            if (bidPrice > 250) {
                int cityIdInt = Integer.valueOf(dataArray[7]);
                String userAgent = dataArray[4];

                cityId.set(cityIdInt);
//                writable.setHighBitPriceImpressions(1);
                writable.setOperationSystem(getOperationSystemName(userAgent));
                log.debug("Mapper: write in context: {} - {}", cityIdInt, writable.toString());
                context.write(cityId, writable);
            }
        }
    }

    private String getOperationSystemName(String userAgent) {
        return userAgentStringParser.parse(userAgent).getOperatingSystem().getName();
    }
}
