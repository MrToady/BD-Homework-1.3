import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class HighBitPricePartitioner extends Partitioner<IntWritable, IntWritable>{

    @Override
    public int getPartition(IntWritable intWritable, IntWritable intWritable2, int numPartitions) {









        //TODO Сделать кастомный варйтабл который будет содержать юзер агент и оперейшн систем и по ней уже партишенер будет выбриарать куда засунуть.
        //TODO 1. Из маппера выходит кастомный врайтбл с оп систем
        //TODO 2. комбайнер не комбайнит????????
        //TODO 3. Партишнер опрделеяет по ОС куда пойдет пара
        //TODO
        return 0;
    }
}