package net.talqum.hadoop.uniqueviewstats;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ‘Big Data’ elemzési eszközök nyílt forráskódú platformokon - Házi Feladat
 * Napi egyedi látogatók (hosztok) száma
 *
 * Created by Imre on 2014.11.12.
 */

/*
public class UniqueSessionDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Unique Session counter");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(UniqueSessionMapper.class);
        job.setReducerClass(UniqueSessionReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new UniqueSessionDriver(), args);
        System.exit(exitCode);
    }
}
*/

public class UniqueSessionDriver{

    public static void main(String[] args) throws Exception{
        Job job = new Job();
        job.setJarByClass(UniqueSessionDriver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(UniqueSessionMapper.class);
        job.setReducerClass(UniqueSessionReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}