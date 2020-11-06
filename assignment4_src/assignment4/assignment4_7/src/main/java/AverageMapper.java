import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import java.text.SimpleDateFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends Mapper<Object, Text, Text, AverageTuple>{

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        Text year = new Text();
        AverageTuple tuple= new AverageTuple();
        if (fields == null || fields.length != 9) {
            System.err.println("Bad data. The expected format is 9 columns seperated by comma.");
            return; // skip the bad data
        }

        try {
            year.set(fields[2].substring(0,4));
            float price = Float.parseFloat(fields[8]);
            tuple.setAverage(price);
            tuple.setCount(1);
        } catch (Exception e) {
            System.err.println("Bad data. Cannot parse the symbol, date, volume and price in column 2, 3, 8 and 9.");
            e.printStackTrace();
            return;
        }

        context.write(year, tuple);
    }
}
