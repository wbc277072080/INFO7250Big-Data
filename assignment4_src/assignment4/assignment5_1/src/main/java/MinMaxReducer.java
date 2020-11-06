import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxReducer extends Reducer<Text, MinMaxWritable, Text, MinMaxWritable>{
    @Override
    public void reduce(Text key, Iterable<MinMaxWritable> values, Context context)
            throws IOException, InterruptedException {

//            move outside of reduce() to get the global max/min
        //MinMaxWritable max = new MinMaxWritable();
        //MinMaxWritable min = new MinMaxWritable();
        int maxv = -1;
        int minv= -1;
        Double maxclose=0.0;
        MinMaxWritable output = new MinMaxWritable();

        for(MinMaxWritable stock: values) {

            int volume1 = stock.getMaxVolume();
            int volume2 = stock.getMinVolume();
            Double close = stock.getMaxClose();

            if (maxv == -1 || maxv < volume1) {
                maxv=volume1;

            }
            if (minv == -1 || minv > volume2) {
               minv=volume2;
            }
            if (maxclose == 0.0 || maxclose < close) {
                maxclose=close;


            }

        }

        output.setMaxVolume(maxv);
        output.setMinVolume(minv);
        output.setMaxClose(maxclose);

        context.write(key, output);
    }

}
