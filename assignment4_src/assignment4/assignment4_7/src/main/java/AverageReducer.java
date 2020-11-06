import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
public class AverageReducer extends Reducer<Text, AverageTuple, Text, AverageTuple> {
    private AverageTuple tuple = new AverageTuple();

    @Override
    public void reduce(Text key, Iterable<AverageTuple> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;
        long sum = 0;

        for (AverageTuple v: values) {
            count += v.getCount(); // total count
            sum += v.getCount() * v.getAverage(); // running sum
        }
        tuple.setCount(count);
        float averagePrice = (float)1.0*sum/count;
        tuple.setAverage(averagePrice);
        context.write(key, tuple);
    }
}
