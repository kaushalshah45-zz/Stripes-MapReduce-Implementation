package stripes.stripes;


import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Stripes {

  public static class StripeMapper extends Mapper<Object, Text, Text, MapWritable>{
	  

        private HashMap<Integer, String> stopWords = StopWords.initializeStopWordHash();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

	    	String line = value.toString();
	        line = line.toLowerCase();
			line = line.replaceAll("[^a-zA-Z]+", " ");
			line = line.replaceAll("^\\s+", "");
			String[] tokens = line.split("\\s+");
			MapWritable h = new MapWritable();
			for (int i = 0; i < tokens.length; i++) {
				h.clear();
				if(stopWords.containsValue(tokens[i])){
					continue;
				}
				for (int j = 0; j < tokens.length; j++) {
					if (i == j || stopWords.containsValue(tokens[j])) {
						continue;
					}
					Text neighbor = new Text(tokens[j]);
	                if(h.containsKey(neighbor)){
	                   IntWritable count = (IntWritable)h.get(neighbor);
	                   count.set(count.get()+1);
	                }else{
	                   h.put(neighbor,new IntWritable(1));
	                }
				}
				context.write(new Text(tokens[i]), h);
			}
        }
    }
  

  public static class StripeReducer extends Reducer<Text,MapWritable,Text,Text> {
    
	  private MapWritable result = new MapWritable();
	  public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			
		  result.clear();
		  for (MapWritable val : values) {
			 
			  Set<Writable> keys = val.keySet();
			  for (Writable final_key : keys) {
				  
				  IntWritable freq = (IntWritable) val.get(final_key);
				  if (result.containsKey(final_key)) {
					  IntWritable count = (IntWritable) result.get(final_key);
					  count.set(count.get() + freq.get());
				  } else {
					  result.put(final_key, freq);
				  }
			  }
		  }
			
		  Set<Writable> att = result.keySet();
		  String output = "";
		  for(Writable a : att){
			  IntWritable freq = (IntWritable) result.get(a);
			  output += a.toString() + " : " + freq +",  ";
		  }
		  context.write(key, new Text(output));
		  context.write(new Text("\n"), new Text(""));
	  }
	  
  }

  public static void main(String[] args) throws Exception {
	  
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "stripes");
	    job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
	    job.setJarByClass(Stripes.class);
	    job.setMapperClass(StripeMapper.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(MapWritable.class);
	    job.setReducerClass(StripeReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}