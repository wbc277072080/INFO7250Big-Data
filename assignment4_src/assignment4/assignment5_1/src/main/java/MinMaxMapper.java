import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class MinMaxMapper extends Mapper<Object, Text, Text, MinMaxWritable>{
    //MinMaxWritable stock = null;

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        MinMaxWritable stock = new MinMaxWritable(value.toString());

         if (stock != null) {
            context.write(new Text(stock.getSymbol()), stock);
        } else {
            System.out.println("Map: One MinMaxWritable is null");
        }
    }
}
