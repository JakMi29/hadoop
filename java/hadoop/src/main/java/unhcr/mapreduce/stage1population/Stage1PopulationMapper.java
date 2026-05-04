package unhcr.mapreduce.stage1population;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Stage1PopulationMapper extends Mapper<LongWritable, Text, Text, Text> {

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
        String refugees = cols[9];
        String idps = cols[12];

        // zamien "-" na 0
        if (refugees.equals("-") || refugees.isEmpty()) refugees = "0";
        if (idps.equals("-") || idps.isEmpty()) idps = "0";
        context.write(new Text(coo_id),
                new Text(year + "," + refugees + "," + idps));
    }
}
