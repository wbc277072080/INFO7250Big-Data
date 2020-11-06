import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class SMapper extends Mapper<Object, Text, Text, IntWritable> {

    public void map (Object key,Text value, Context context)
            throws IOException, InterruptedException{
        String[] inputSplit = value.toString().split("\t");
        String pos = inputSplit[0];
        IntWritable one = new IntWritable(1);

        context.write(new Text(pos),one);
    }
}
