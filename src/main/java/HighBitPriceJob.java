import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class HighBitPriceJob {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "High Bit Price counter");

        job.setJarByClass(HighBitPriceJob.class);
        job.setMapperClass(HighBitPriceMapper.class);
        job.setReducerClass(HighBitPriceReducer.class);
        job.setCombinerClass(HighBitPriceCombiner.class);
        job.setSortComparatorClass(IntWritable.Comparator.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setNumReduceTasks(Runtime.getRuntime().availableProcessors() - 1);
        job.addCacheFile(URI.create("/tmp/taras/city.en.txt"));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
