package unhcr.mapreduce.stage2;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class Stage2Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        String popLine = null;
        String demoLine = null;

        // rozdzielenie źródeł
        for (Text val : values) {
            String v = val.toString();

            if (v.startsWith("P|")) {
                popLine = v.substring(2);
            } else if (v.startsWith("D|")) {
                demoLine = v.substring(2);
            }
        }

        // jeśli brakuje danych → skip
        if (popLine == null || demoLine == null) return;

        try {
            String[] p = popLine.split(",");
            String[] d = demoLine.split(",");

            String year = p[0];
            String coo_id = p[1];

            int refugees = Integer.parseInt(p[2]);
            int idps = Integer.parseInt(p[3]);
            int ref_prev = Integer.parseInt(p[4]);
            int idps_prev = Integer.parseInt(p[5]);

            int total = Integer.parseInt(d[2]);
            int m_18_59 = Integer.parseInt(d[3]);

            // ===== METRYKI =====

            double fit_for_duty = (total == 0) ? 0 :
                    ((double) m_18_59 / total) * 100.0;

            double percent_diff_refugees = (ref_prev == 0) ? 0 :
                    ((double) (refugees - ref_prev) / ref_prev) * 100.0;

            double percent_diff_idps = (idps_prev == 0) ? 0 :
                    ((double) (idps - idps_prev) / idps_prev) * 100.0;

            // ===== OUTPUT =====

            String output = year + "," + coo_id + ","
                    + refugees + "," + idps + ","
                    + total + "," + m_18_59 + ","
                    + fit_for_duty + ","
                    + percent_diff_refugees + ","
                    + percent_diff_idps;

            context.write(null, new Text(output));

        } catch (Exception e) {
            // możesz zalogować
        }
    }
}