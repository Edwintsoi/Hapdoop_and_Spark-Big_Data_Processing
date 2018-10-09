

import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

enum CustomCounters {NUM_MEDALIST}

public class TwitterC {

  public static void runJob_Medalists(String[] input, String output) throws Exception {

    Configuration conf = new Configuration();

    Job job = new Job(conf);
    job.setJarByClass(Tokenizer.class);
    job.setMapperClass(TokenizerMapper.class);
    
 

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setNumReduceTasks(0;

    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.addCacheFile(new Path("/data/medalistsrio.csv").toUri());

    Path outputPath = new Path(output);
    FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
    FileOutputFormat.setOutputPath(job, outputPath);
    outputPath.getFileSystem(conf).delete(outputPath,true);
    job.waitForCompletion(true);
  }

  public static void runJob_Popularity(String[] input, String output) throws Exception {

    Configuration conf = new Configuration();

    Job job = new Job(conf);
    job.setJarByClass(TwitterC.class);
    job.setMapperClass(TwitterCTokenizerMapper.class);
    job.setReducerClass(TwitterCReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setNumReduceTasks(1);

    Path outputPath = new Path(output);
    FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
    FileOutputFormat.setOutputPath(job, outputPath);
    outputPath.getFileSystem(conf).delete(outputPath,true);
    job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {

       runJob_Medalists(Arrays.copyOfRange(args, 0, 0 ) , args[1] );   //medalist_file medalist_output

       //runJob_Popularity(Arrays.copyOfRange(args, 2, 2 ) , args[3] );  //medalist_output tweet_file output
  }

}
