import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
public class NyseMapper extends Mapper<LongWritable, Text, Text, MinMaxTuple > {
    MinMaxTuple minMaxTuple = new MinMaxTuple();
    Text deptKey = new Text();

    protected void map(LongWritable key, Text value, Context context)
            throws IOException,
            InterruptedException, IOException {
        String line = value.toString();
        String[] tokens = line.split(",",-1);
        double high = 0.0;

        if(tokens[4].length()>0){
            high = Double.parseDouble(tokens[4]);
            minMaxTuple.setMax(high);
            deptKey.set(tokens[1]);
        }
        context.write(deptKey, minMaxTuple);
    }
}
