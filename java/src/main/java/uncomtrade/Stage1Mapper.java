package uncomtrade;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Stage1Mapper extends Mapper<LongWritable, Text, Text, Text> {

    private boolean isHeader = true;

    @Override
    protected void setup(Context context) {
        isHeader = true;
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        if (isHeader) {
            isHeader = false;
            return;
        }

        String[] cols = value.toString().split(",");
        if (cols.length <= 42) return;

        String reporterCode = cols[6].trim();
        String refYear      = cols[3].trim();
        String cmdCode      = cols[20].trim();
        String primaryValue = cols[42].trim();

        if (reporterCode.isEmpty() || refYear.isEmpty()) return;

        double val = 0;
        try { val = Double.parseDouble(primaryValue); } catch (Exception e) { val = 0; }

        context.write(
            new Text(reporterCode + "," + refYear),
            new Text(cmdCode + "," + val)
        );
    }
}
