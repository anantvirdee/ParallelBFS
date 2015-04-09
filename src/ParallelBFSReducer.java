import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class ParallelBFSReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
	
	StringBuilder reduce_out	=	new StringBuilder();
	int distance = Integer.MAX_VALUE;
	String color = "WHITE";
	String neighbours;
	@Override
	
	public void reduce(IntWritable key,Iterable<Text> values,Context context) throws IOException,InterruptedException
	{
		 
		for(Text value : values){
			//System.out.println(value);
			String[] split_val = value.toString().split("\\|");
			
			if(!split_val[1].equals("null")){
				neighbours=split_val[1];
			}
			if(distance>(Integer.parseInt(split_val[2]))){
				distance=Integer.parseInt(split_val[2]);
			}
			if(split_val[3].equals("BLACK")){
				color = "BLACK";
				
			}
			else if (!color.equals("BLACK")) {
				if(split_val[3].equals("GRAY")){
					color="GRAY";
				}
			}			
		 }
		
		reduce_out.append('|');
    	reduce_out.append(neighbours);
    	reduce_out.append('|');
    	reduce_out.append(distance);
    	reduce_out.append('|');
    	reduce_out.append(color);
    	reduce_out.append('|');
		context.write(key, new Text(reduce_out.toString()));
		reduce_out.setLength(0);
		color="WHITE";
		distance=Integer.MAX_VALUE;
		
	}

}
