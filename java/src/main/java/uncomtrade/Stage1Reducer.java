package uncomtrade;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Ten sam Reducer służy jako Combiner (SUM jest łączny i przemienny)
public class Stage1Reducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        double tradeTotal = 0, fuelValue = 0, grainValue = 0, gunValue = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",", 2);
            if (parts.length < 2) continue;

            String cmdCode = parts[0].trim();
            double v = 0;
            try { v = Double.parseDouble(parts[1].trim()); } catch (Exception e) { v = 0; }

            tradeTotal += v;
            if (cmdCode.equals("27")) fuelValue  += v;
            if (cmdCode.equals("12")) grainValue += v;
            if (cmdCode.equals("93")) gunValue   += v;
        }

        // format: reporterCode,year,trade_total,fuel_value,grain_value,gun_value
        String out = key.toString() + "," + tradeTotal + "," + fuelValue + "," + grainValue + "," + gunValue;
        context.write(null, new Text(out));
    }
}
