package uncomtrade;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Stage3Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().trim();
        if (line.isEmpty()) return;

        String[] cols = line.split(",");
        if (cols.length < 5) return;

        String reporterCode = cols[0].trim();
        String year         = cols[1].trim();


        String paddedCode = String.format("%010d", Long.parseLong(reporterCode));
        String paddedYear = String.format("%04d", Integer.parseInt(year));

        context.write(new Text(paddedCode + "_" + paddedYear), new Text(line));
    }
}
