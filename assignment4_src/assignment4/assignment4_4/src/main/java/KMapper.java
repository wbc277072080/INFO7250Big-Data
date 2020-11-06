import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class KMapper extends Mapper<Object, Text, Text, IntWritable> {
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] inputSplit = value.toString().split("\t");
        String pos = inputSplit[0];
        IntWritable ONE = new IntWritable(1);
        context.write(new Text(pos), ONE);
    }
}
