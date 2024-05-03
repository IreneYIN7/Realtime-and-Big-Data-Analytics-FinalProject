import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataOutput;
import java.io.DataInput;
public class CumulativeWritable implements  Writable{
    private long cumulative_persons_vaccinated;
    private long cumulative_persons_fully_vaccinated;
    private long cumulative_vaccine_doses_administered;
    public void setCumulative_persons_vaccinated(long num){
        this.cumulative_persons_vaccinated = num;
    }
    public long getCumulative_persons_vaccinated(){
        return this.cumulative_persons_vaccinated;
    }

    public void setCumulative_persons_fully_vaccinated(long num){
        this.cumulative_persons_fully_vaccinated = num;
    }
    public long getCumulative_persons_fully_vaccinated(){
        return this.cumulative_persons_fully_vaccinated;
    }
    public void setCumulative_vaccine_doses_administered(long num){
        this.cumulative_vaccine_doses_administered = num;
    }
    public long getCumulative_vaccine_doses_administered(){
        return this.cumulative_vaccine_doses_administered;
    }

    public void readFields(DataInput in)throws IOException{
        cumulative_persons_vaccinated = in.readLong();
        cumulative_persons_fully_vaccinated = in.readLong();
        cumulative_vaccine_doses_administered = in.readLong();
    }
    public static CumulativeWritable read(DataInput in) throws  IOException{
        CumulativeWritable tuple = new CumulativeWritable();
        tuple.readFields(in);
        return tuple;
    }
    @Override
    public void write(DataOutput out) throws IOException{
        //out.writeBytes(date);
        out.writeLong(cumulative_persons_vaccinated);
        out.writeLong(cumulative_persons_fully_vaccinated);
        out.writeLong(cumulative_vaccine_doses_administered);
    }
    @Override
    public String toString(){
        return getCumulative_persons_vaccinated()+","+getCumulative_persons_fully_vaccinated()+","+getCumulative_vaccine_doses_administered();
    }


}
