package mapreduce.assignment.task1;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvalidRecordsMapper extends Mapper<LongWritable,Text,Text,Text>{
	public void map(LongWritable Key,Text Value,Context context) throws IOException, InterruptedException{
		String[] lineArray = Value.toString().split(Pattern.quote("|"));
		
		if(lineArray[0].equals("NA") || (lineArray[1].equals("NA"))){
			context.write(new Text(""), Value);
		}
	}

}
