import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class MissingCards {
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	    public void map(LongWritable lw, Text text, Context context)
	    		throws IOException, InterruptedException{
	        String suitsAndRanks = text.toString();
	        String[] split = suitsAndRanks.split(" ");
	        context.write(new Text(split[0]), new IntWritable(Integer.parseInt(split[1])));
	    }
	}
	    
	public static class Reduce 
		extends Reducer<Text, IntWritable, Text, IntWritable>{
		/**
		 * Number of maximum rank, 13.
		 */
		final int MAX_RANK_NUMBER = 13;
		
		/**
		 * Sum of all ranks from one suit.
		 */
		final int ONE_SUIT_SUM = 91;
		
		public void reduce(Text text, Iterable<IntWritable> cardDeck, Context context) 
				throws IOException, InterruptedException{
			ArrayList<Integer> ranks = new ArrayList<Integer>();
	    	int sum = 0;
	    	int currentCard = 0;
	    	for (IntWritable card : cardDeck) {
	    		sum += card.get();
	    		currentCard = card.get();
	    		ranks.add(currentCard);
	    	}
	   
	    	if(sum < ONE_SUIT_SUM){
	    		for(int i = 1; i <= MAX_RANK_NUMBER; i++){
	    			if(!ranks.contains(i)){
	    				context.write(text, new IntWritable(i));
	    			}
	    		}
	    	}
		}  
	}
	 	
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
	    Job job = new Job(conf, "MissingCards");
	    job.setJarByClass(MissingCards.class);
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
