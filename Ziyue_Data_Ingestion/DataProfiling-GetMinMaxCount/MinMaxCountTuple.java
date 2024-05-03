import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataOutput;
import java.io.DataInput;

public class MinMaxCountTuple implements Writable{
    private long min;
    private long max;
    private int count;
    public void setMin(long min){
        this.min = min;
    }
    public void setMax(long max){
        this.max = max;
    }
    public void setCount(int count){
        this.count = count;
    }
    public long getMin(){
        return this.min;
    }
    public long getMax(){
        return this.max;
    }
    public int getCount(){
        return this.count;
    }
    @Override
    public void write(DataOutput out) throws IOException{
        out.writeLong(min);
        out.writeLong(max);
        out.writeInt(count);
    }
    @Override
    public String toString(){
        return getMin()+ "\t"+getMax()+"\t"+getCount();
    }

    public void readFields(DataInput in) throws IOException{
    	min = in.readLong();
	max = in.readLong();
	count = in.readInt();
    }

  
    public static MinMaxCountTuple read(DataInput in) throws IOException{
    	MinMaxCountTuple tuple = new MinMaxCountTuple();
	tuple.readFields(in);
	return tuple;
    }
    
}
