package unhcr.mapreduce.stage1population;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class Stage1PopulationReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Map<Integer, int[]> data = new TreeMap<>();

        // agregacja
        for (Text val : values) {
            String[] parts = val.toString().split(",");
            int year = Integer.parseInt(parts[0]);
            int refugees;
            int idps;

            try {
                refugees = Integer.parseInt(parts[1]);
            } catch (Exception e) {
                refugees = 0;
                System.out.println("BLAD W refugees: "+ parts[1].toString());
            }

            try {
                idps = Integer.parseInt(parts[2]);
            } catch (Exception e) {
                idps = 0;
                System.out.println("BLAD W idps: "+ parts[2].toString());
            }

            data.putIfAbsent(year, new int[]{0, 0});
            data.get(year)[0] += refugees;
            data.get(year)[1] += idps;
        }

        int prevRefugees = 0;
        int prevIdps = 0;

        for (Map.Entry<Integer, int[]> entry : data.entrySet()) {
            int year = entry.getKey();
            int refugees = entry.getValue()[0];
            int idps = entry.getValue()[1];

            String output = year + "," + key.toString() + ","
                    + refugees + "," + idps + ","
                    + prevRefugees + "," + prevIdps;

            context.write(null, new Text(output));

            prevRefugees = refugees;
            prevIdps = idps;
        }
    }
}