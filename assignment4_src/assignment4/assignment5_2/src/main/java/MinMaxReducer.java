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
        Date maxDate=new Date();
        Date minDate=new Date();
        MinMaxWritable output = new MinMaxWritable();

        for(MinMaxWritable stock: values) {

            int volume = stock.getVolume();
            Double close = stock.getMaxClose();

            if (maxv == -1 || maxv < volume) {
                maxv=volume;
                //output.setMaxVolumeDate(stock.getMaxVolumeDate());
                maxDate=new Date(stock.getMaxVolumeDate().getTime());

            }
            if (minv == -1 || minv > volume) {
                minv=volume;
                //output.setMinVolumeDate(stock.getMinVolumeDate());
                minDate=new Date(stock.getMinVolumeDate().getTime());
            }
            if (maxclose == 0.0 || maxclose < close) {
                maxclose=close;


            }

        }
        output.setMaxVolumeDate(maxDate);
        output.setMinVolumeDate(minDate);
        output.setMaxClose(maxclose);

        context.write(key, output);
    }

}
