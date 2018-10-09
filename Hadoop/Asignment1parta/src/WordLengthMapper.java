import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordLengthMapper extends
		Mapper<Object, Text, IntWritable, IntWritable> {

	private final IntWritable one = new IntWritable(1);
	private final IntWritable length = new IntWritable();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
String[] line=value.toString().split(";");

if(line.length== 4){
	String Box = line[2];

	if( ( Box.length() > 0 ) & (Box.length() <= 140)){
			int rangenumber = (int)( ( Box.length() - 1 ) /5);
			length.set(rangenumber);
			context.write(length, one);
	}
}
}
}
