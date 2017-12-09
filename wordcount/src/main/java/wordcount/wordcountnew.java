package wordcount;

import java.io.IOException;
import java.util.StringTokenizer;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class wordcountnew {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{
	//hadoop supported data types for the key/value pairs
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    //map method that performs the tokenizer job
    // & framing the initial key value pairs
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	//taking one line at a time and tokenizing the same
    	StringTokenizer itr = new StringTokenizer(value.toString());
    	//iterating through all the words available in that line
    	//and forming the key value pair
    	while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        //sending to output collector which in turn passes the same to reducer
        context.write(word, one);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable(); // type of output value
  //reduce method accepts the Key Value pairs from mappers,
  //do the aggregation based on keys and produce the final output
    
    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0; // initialize the sum for each keyword
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      /*iterates through all the values available with a key
      and add them together and give the
      final result as the key and sum of its values*/
      context.write(key, result); // create a pair <keyword, number of occurrence>
    }
  }

  public static void main(String[] args) throws Exception {
	//creating a JobConf object and assigning a job name for identification
	//purposes
	  Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    //set class for mapper and reducer
    job.setJarByClass(wordcountnew.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    
    //set output format
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
  //set the HDFS path of the input data
    FileInputFormat.addInputPath(job, new Path(args[0]));
 // set the HDFS path for the output
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
