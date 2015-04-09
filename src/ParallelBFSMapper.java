import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class ParallelBFSMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
	
	private IntWritable mapKeyOut	=	new IntWritable();
	//private Text mapValueOut		=	new Text();
	private String color			=	new String();
	private StringBuilder out		= 	new StringBuilder();

	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
		
		String line 		= value.toString();
	    String[] split_val	= line.split("\\|");
	    color				= split_val[3];
	    
	    if(color.equals("GRAY")){
	    	String[] neighbor_nodes = split_val[1].split(",");
	    	
	    	// Output current node (key,value) with color BLACK
	    	mapKeyOut.set(Integer.parseInt(split_val[0].substring(0, 1)));
	    	out.append('|');
	    	out.append(split_val[1]);
	    	out.append('|');
	    	out.append(split_val[2]);
	    	out.append('|');
	    	out.append("BLACK");
	    	out.append('|');
	    	context.write(mapKeyOut, new Text(out.toString()));
	    	
	    	// Output neighbor nodes(key,value) with neighbour's NULL, Distance= Distance of current_node + 1, color GRAY
	    	for(int i=0;i<neighbor_nodes.length;i++){
	    		out.setLength(0);
	    		mapKeyOut.set(Integer.parseInt(neighbor_nodes[i]));
	    		out.append('|');
		    	out.append("null");
		    	out.append('|');
		    	out.append(Integer.parseInt(split_val[2]) + 1);
		    	out.append('|');
		    	out.append("GRAY");
		    	out.append('|');
		    	context.write(mapKeyOut, new Text(out.toString()));
		    	
	    	}
			
	    	
	    }
	    // For nodes with color either BLACK or WHITE
	    else{
	    	//context.write(key, value);
	    	mapKeyOut.set(Integer.parseInt(split_val[0].substring(0, 1)));
	    	out.append('|');
	    	out.append(split_val[1]);
	    	out.append('|');
	    	out.append(split_val[2]);
	    	out.append('|');
	    	out.append(split_val[3]);
	    	out.append('|');
	    	context.write(mapKeyOut, new Text(out.toString()));
	    	out.setLength(0);
	    }
		
	}

}
