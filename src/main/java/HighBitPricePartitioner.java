import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class HighBitPricePartitioner extends Partitioner<HighBitOSWritable, IntWritable> {

    /**
     * @return number of reducer basing on Operation System Name from value
     * @see {@link HighBitOSWritable}
     */
    @Override
    public int getPartition(HighBitOSWritable key, IntWritable value, int numReduceTasks) {
        return (key.getOperationSystem().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}