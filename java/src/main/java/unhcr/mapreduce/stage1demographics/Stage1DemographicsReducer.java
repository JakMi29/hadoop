package unhcr.mapreduce.stage1demographics;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class Stage1DemographicsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Map<Integer, int[]> data = new TreeMap<>();

        // agregacja
        for (Text val : values) {
            String[] parts = val.toString().split(",");

            int year;
            int total;
            int m_18_59;

            try {
                year = Integer.parseInt(parts[0]);
            } catch (Exception e) {
                continue; // pomijamy błędny rekord
            }

            try {
                total = Integer.parseInt(parts[1]);
            } catch (Exception e) {
                total = 0;
            }

            try {
                m_18_59 = Integer.parseInt(parts[2]);
            } catch (Exception e) {
                m_18_59 = 0;
            }

            // 🔥 KLUCZOWE: agregacja do mapy
            data.putIfAbsent(year, new int[]{0, 0});
            data.get(year)[0] += total;
            data.get(year)[1] += m_18_59;
        }

        // zapis wyników
        for (Map.Entry<Integer, int[]> entry : data.entrySet()) {
            int year = entry.getKey();
            int total = entry.getValue()[0];
            int m_18_59 = entry.getValue()[1];

            String output = year + "," + key.toString() + "," + total + "," + m_18_59;

            context.write(null, new Text(output));
        }
    }
}