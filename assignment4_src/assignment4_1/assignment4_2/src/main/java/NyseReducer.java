import org.apache.commons.math3.analysis.function.Min;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
public class NyseReducer extends Reducer<Text, MinMaxTuple, Text, MinMaxTuple>{
    private MinMaxTuple result= new MinMaxTuple();
    protected void reduce(Text key, Iterable<MinMaxTuple> values, Context context)
            throws IOException,
            InterruptedException, IOException {
        result.setMax(null);
        List<Double> high_prices = new ArrayList<Double>();
        for(MinMaxTuple minMaxTuple : values)
        {
            high_prices.add(minMaxTuple.getMax());
            if(result.getMax() == null || minMaxTuple.getMax()>result.getMax()){
                result.setMax(minMaxTuple.getMax());
            }
        }
        context.write(key,result);
    }
}
