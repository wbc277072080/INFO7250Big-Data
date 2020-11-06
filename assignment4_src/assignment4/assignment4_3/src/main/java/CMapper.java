import java.io.IOException;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
public class CMapper extends Mapper<Object, BytesWritable, Text, IntWritable >{
    private Text word = new Text();

    @Override
    public void map(Object key, BytesWritable value, Context context) throws IOException, InterruptedException {
        word.set(value.getBytes());
        IntWritable ONE = new IntWritable(1);
        context.write(word, ONE);
    }
}
