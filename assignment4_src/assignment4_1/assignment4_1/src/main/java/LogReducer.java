import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
public class LogReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException,
            InterruptedException {
        int urlcount = 0;
        for(IntWritable val : values)
        {
            urlcount+=val.get();
        }
        context.write(key, new IntWritable(urlcount));
    }
}
