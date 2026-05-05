package uncomtrade;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Stage2Mapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] cols = value.toString().split(",");
        if (cols.length < 6) return;

        String reporterCode = cols[0].trim();
        String year         = cols[1].trim();

        double tradeTotal = 0, fuelValue = 0, grainValue = 0, gunValue = 0;
        try { tradeTotal = Double.parseDouble(cols[2].trim()); } catch (Exception e) {}
        try { fuelValue  = Double.parseDouble(cols[3].trim()); } catch (Exception e) {}
        try { grainValue = Double.parseDouble(cols[4].trim()); } catch (Exception e) {}
        try { gunValue   = Double.parseDouble(cols[5].trim()); } catch (Exception e) {}

        double fuelShare  = tradeTotal > 0 ? fuelValue  / tradeTotal : 0;
        double grainShare = tradeTotal > 0 ? grainValue / tradeTotal : 0;
        double gunShare   = tradeTotal > 0 ? gunValue   / tradeTotal : 0;


        String out = reporterCode + "," + year + "," + fuelShare + "," + grainShare + "," + gunShare;
        context.write(new Text(out), null);
    }
}
