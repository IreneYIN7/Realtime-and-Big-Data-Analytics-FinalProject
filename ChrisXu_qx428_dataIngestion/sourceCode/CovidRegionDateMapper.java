
// hadoop mapper
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;
import java.util.*;

public class CovidRegionDateMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(",", -1);

        String date = columns[0];
        String location = columns[1];

        String newHospitalizedPatients = columns[2];
        String cumulativeHospitalizedPatients = columns[3];

        String newIntensiveCarePatients = columns[5];
        String cumulativeIntensiveCarePatients = columns[6];

        Text outputValue = new Text(newHospitalizedPatients + "," +
                cumulativeHospitalizedPatients + "," +
                newIntensiveCarePatients + "," +
                cumulativeIntensiveCarePatients);

        if (location.length() == 2) { // country
            // key = date + location
            Text outPutKey = new Text(location + "," + date);
            context.write(outPutKey, outputValue);

        } else if (location.startsWith("US") && location.length() == 5) { // US states
            Text outPutKey = new Text(location.substring(0, 5) + "," + date);
            context.write(outPutKey, outputValue);

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
