
// hadoop mapper
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import java.util.*;

public class CovidRegionOnlyMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(",", -1);

        if (columns.length > 5) { // hospitalization data
            String location = columns[1];

            String newHospitalizedPatients = columns[2];

            String newIntensiveCarePatients = columns[5];

            Text outputValue = new Text("CASES:" + newHospitalizedPatients + "," + newIntensiveCarePatients);
            if (location == "AR") {
                System.out.println("new econ: " + columns[2] + " " + columns[5]);
            }
            if (location.length() == 2 || (location.startsWith("US") && location.length() == 5)) {
                // key = date + location
                Text outPutKey = new Text(location);
                context.write(outPutKey, outputValue);

            }
        } else { // econ data
            String location = columns[0];
            String gdp = columns[1];
            String gdp_per = columns[2];
            String hci = columns[3];
            
            if (location == "AR") {
                System.out.println("new econ: " + gdp + " " + gdp_per + " " + hci);
            }
            Text outputValue = new Text("ECON:" + gdp + "," + gdp_per + "," + hci);
            if (location.length() == 2 || (location.startsWith("US") && location.length() == 5)) {
                // if (columns.length < 4) {
                //     System.out.println("econ1: " + value.toString());
                // }
                Text outPutKey = new Text(location);
                context.write(outPutKey, outputValue);
            }
        }
    }
}

// date,
// location_key,
// new_hospitalized_patients,
// cumulative_hospitalized_patients,
// current_hospitalized_patients,
// new_intensive_care_patients,
// cumulative_intensive_care_patients,
// current_intensive_care_patients,
// new_ventilator_patients,
// cumulative_ventilator_patients,
// current_ventilator_patients
