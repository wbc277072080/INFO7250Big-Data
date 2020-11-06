import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AverageTuple implements Writable {
    private int count = 0;
    private float average;

    public void readFields(DataInput in) throws IOException {
        count = in.readInt();
        average = in.readFloat();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeFloat(average);
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public float getAverage() {
        return average;
    }

    public void setAverage(float average) {
        this.average = average;
    }

    @Override
    public String toString() {
        return count + "\t" + average;
    }



}