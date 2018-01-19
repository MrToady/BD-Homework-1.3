import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class HighBitPricePartitioner extends Partitioner<IntWritable, HighBitOSWritable>{

    @Override
    public int getPartition(IntWritable key, HighBitOSWritable value, int numReduceTasks) {
        return (value.getOperationSystem().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}