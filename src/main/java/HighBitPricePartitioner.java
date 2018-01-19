import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class HighBitPricePartitioner extends Partitioner<IntWritable, HighBitOSWritable> {

    /**
     * @return number of reducer basing on Operation System Name from value
     * @see {@link HighBitOSWritable}
     */
    @Override
    public int getPartition(IntWritable key, HighBitOSWritable value, int numReduceTasks) {
        return (value.getOperationSystem().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}