import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
public class CombineReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException,
            InterruptedException {
        int symblecount = 0;
        for(IntWritable val : values)
        {
            symblecount+=val.get();
        }
        context.write(key, new IntWritable(symblecount));
    }
}
