import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxTuple implements Writable{
    private Double max;

    public MinMaxTuple() {
    }



    public void write(DataOutput out) throws IOException {
        out.writeDouble(max);
    }

    public void readFields(DataInput in) throws IOException {
        max = in.readDouble();
    }


    public Double getMax() {
        return max;
    }

    public void setMax(Double max) {
        this.max = max;
    }


    public String toString()
    {
        return max.toString();
    }
}
