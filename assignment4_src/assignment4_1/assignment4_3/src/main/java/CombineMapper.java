import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
public class CombineMapper extends Mapper<Object, Text, Text, IntWritable >{
    public void map (Object key,Text value, Context context)
            throws IOException, InterruptedException{
        String[] inputSplit = value.toString().split(",");
        String symble = inputSplit[1];
        IntWritable one = new IntWritable(1);

        context.write(new Text(symble),one);
    }
}
