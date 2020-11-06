import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class LogMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map (Object key,Text value, Context context)
            throws IOException, InterruptedException{
        String[] inputSplit = value.toString().split(" ");
        String url = inputSplit[0];
        IntWritable one = new IntWritable(1);

        context.write(new Text(url),one);
    }
}
