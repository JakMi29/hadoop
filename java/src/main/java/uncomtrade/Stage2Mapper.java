package uncomtrade;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// p3.0 -> p3.1: liczy udziały (shares). Brak Reducera (numReduceTasks=0).
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

        // format: reporterCode,year,fuel_share,grain_share,gun_share
        String out = reporterCode + "," + year + "," + fuelShare + "," + grainShare + "," + gunShare;
        context.write(new Text(out), null);  // klucz=dane, wartość=null (brak reducera)
    }
}
