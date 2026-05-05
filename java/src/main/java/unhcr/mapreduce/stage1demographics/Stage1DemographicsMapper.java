package unhcr.mapreduce.stage1demographics;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Stage1DemographicsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private boolean isHeader = true;

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        if (isHeader) {
            isHeader = false;
            return;
        }

        String[] cols = line.split(",");

        String year = cols[0];
        String coo_id = cols[1];
        String total = cols[23];
        String m_18_59 = cols[19];

        // zamien "-" na 0
        if (total.equals("-") || total.isEmpty()) total = "0";
        if (m_18_59.equals("-") || m_18_59.isEmpty()) m_18_59 = "0";
        context.write(new Text(coo_id),
                new Text(year + "," + total + "," + m_18_59));
    }
}
