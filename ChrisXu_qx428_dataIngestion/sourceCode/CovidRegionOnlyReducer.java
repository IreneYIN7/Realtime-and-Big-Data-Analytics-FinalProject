
// hadoop reducer
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;

public class CovidRegionOnlyReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int newHospitalizedPatients = 0;
        int newIntensiveCarePatients = 0;

        String gdp = "";
        String gdp_per = "";
        String hci = "";

        for (Text value : values) {
            if (value.toString().startsWith("CASES:")) {
                String data = value.toString().split(":")[1];
                String[] columns = data.split(",", -1);
                if (key.toString().equals("AR")) {
                    System.out.println("case: " + value.toString());
                }
                if (!columns[0].isEmpty()) {
                    newHospitalizedPatients += Integer.parseInt(columns[0]);
                }
                if (!columns[1].isEmpty()) {
                    newIntensiveCarePatients += Integer.parseInt(columns[1]);
                }
            } else {
                String[] columns = value.toString().split(":")[1].split(",", -1);
                if (key.toString().equals("AR")) {
                    System.out.println("econ: " + value.toString());
                }
                if (columns.length < 3) {
                    System.out.println("econ not enough: " + value.toString());
                }
                gdp = columns[0];
                gdp_per = columns[1];
                hci = columns[2];

            }
        }

        Text outputValue = new Text(
                gdp + "," + gdp_per + "," + hci + "," + newHospitalizedPatients + "," + newIntensiveCarePatients);
        context.write(key, outputValue);

    }
}
