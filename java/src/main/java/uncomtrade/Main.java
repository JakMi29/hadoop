package uncomtrade;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import uncomtrade.Stage1Mapper;
import uncomtrade.Stage1Reducer;
import uncomtrade.Stage2Mapper;
import uncomtrade.Stage3Mapper;
import uncomtrade.Stage3Reducer;

public class Main {

    // Ścieżki HDFS
    static final String INPUT_DIR  = "/data/un_comtrade";       // tu leżą 1997.csv ... 2025.csv
    static final String OUT_STAGE1 = "/uncomtrade/output/stage1";  // p3.0
    static final String OUT_STAGE2 = "/uncomtrade/output/stage2";  // p3.1
    static final String OUT_STAGE3 = "/uncomtrade/output/stage3";  // p3.2

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // cleanup
        fs.delete(new Path(OUT_STAGE1), true);
        fs.delete(new Path(OUT_STAGE2), true);
        fs.delete(new Path(OUT_STAGE3), true);

        log("START pipeline UN_Comtrade");

        // ─────────────────────────────────────────
        // STAGE 1: z3 → p3.0  (agregacja sum)
        // Hadoop czyta wszystkie pliki z INPUT_DIR równolegle (3 repliki na węzłach).
        // Liczba mapperów = liczba bloków HDFS (każdy plik → osobny mapper lub więcej).
        // Combiner redukuje dane przed siecią.
        // ─────────────────────────────────────────
        Job job1 = Job.getInstance(conf, "UN_Comtrade Stage1: aggregation (z3->p3.0)");
        job1.setJarByClass(Main.class);

        job1.setMapperClass(Stage1Mapper.class);
        job1.setCombinerClass(Stage1Reducer.class);   // Combiner = Reducer (SUM łączny)
        job1.setReducerClass(Stage1Reducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // Cały katalog — Hadoop uruchomi mapper na każdym pliku/bloku równolegle
        FileInputFormat.addInputPath(job1, new Path(INPUT_DIR));
        FileOutputFormat.setOutputPath(job1, new Path(OUT_STAGE1));

        long t1start = System.currentTimeMillis();
        boolean ok1 = job1.waitForCompletion(true);
        long t1end = System.currentTimeMillis();

        log("==== STAGE 1 (z3->p3.0) | Status: " + status(ok1) + " | Czas: " + ms(t1start, t1end));
        if (!ok1) { System.exit(1); }


        // ─────────────────────────────────────────
        // STAGE 2: p3.0 → p3.1  (liczenie shares)
        // Brak Reducera → dane NIE przechodzą przez shuffle/sort.
        // Każdy mapper przetwarza swój kawałek outputu stage1 lokalnie.
        // ─────────────────────────────────────────
        Job job2 = Job.getInstance(conf, "UN_Comtrade Stage2: shares (p3.0->p3.1)");
        job2.setJarByClass(Main.class);

        job2.setMapperClass(Stage2Mapper.class);
        job2.setNumReduceTasks(0);   // brak reducera — tylko map

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);

        FileInputFormat.addInputPath(job2, new Path(OUT_STAGE1));
        FileOutputFormat.setOutputPath(job2, new Path(OUT_STAGE2));

        long t2start = System.currentTimeMillis();
        boolean ok2 = job2.waitForCompletion(true);
        long t2end = System.currentTimeMillis();

        log("==== STAGE 2 (p3.0->p3.1) | Status: " + status(ok2) + " | Czas: " + ms(t2start, t2end));
        if (!ok2) { System.exit(1); }


        // ─────────────────────────────────────────
        // STAGE 3: p3.1 → p3.2  (sortowanie po reporterCode, year)
        // Hadoop sortuje klucze automatycznie podczas shuffle.
        // Używamy zero-padded klucza dla poprawnego sortowania numerycznego.
        // ─────────────────────────────────────────
        Job job3 = Job.getInstance(conf, "UN_Comtrade Stage3: sort (p3.1->p3.2)");
        job3.setJarByClass(Main.class);

        job3.setMapperClass(Stage3Mapper.class);
        job3.setReducerClass(Stage3Reducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(OUT_STAGE2));
        FileOutputFormat.setOutputPath(job3, new Path(OUT_STAGE3));

        long t3start = System.currentTimeMillis();
        boolean ok3 = job3.waitForCompletion(true);
        long t3end = System.currentTimeMillis();

        log("==== STAGE 3 (p3.1->p3.2) | Status: " + status(ok3) + " | Czas: " + ms(t3start, t3end));

        long totalMs = t3end - t1start;
        log("==== PIPELINE TOTAL | Czas: " + totalMs + " ms (" + (totalMs / 1000) + " s)");

        System.exit(ok3 ? 0 : 1);
    }

    private static void log(String msg) {
        System.out.println("[" + java.time.LocalDateTime.now() + "] " + msg);
    }

    private static String status(boolean ok) {
        return ok ? "SUCCESS" : "FAIL";
    }

    private static String ms(long start, long end) {
        return (end - start) + " ms";
    }
}
