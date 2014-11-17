package net.talqum.hadoop.pageviewstats;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * HTML lapletöltések hisztogramja
 * Created by Imre on 2014.11.12..
 *
 * meg kell nézni minden hostra hogy hány lapot nézett
 * mapper -> host,1
 * combiner -> host,x
 * reducer -> host, letöltésszám
 * az azonos számúakat nézőket megszámolni
 */
/*
public class SiteViewStage2Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "Site View counter");
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SiteViewStage2Mapper.class);
        job.setReducerClass(SiteViewStage2Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SiteViewStage2Driver(), args);
        System.exit(exitCode);
    }
}
*/

public class SiteViewStage2Driver{

    public static void main(String[] args) throws Exception{
        Job job = Job.getInstance();
        job.setJarByClass(SiteViewStage2Driver.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(SiteViewStage2Mapper.class);
        job.setReducerClass(SiteViewStage2Reducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}