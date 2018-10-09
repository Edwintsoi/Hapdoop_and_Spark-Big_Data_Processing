import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Iterator;
public class IntSumReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {

    int sum = 0;
        for (IntWritable value : values) {
            sum =value.get()  }
               result.set(sum);
 int rangestart = key.get()*5 +1;
		int rangeend = rangestart + 4;
 Text len = new Text( key.get() + "(" + rangestart + "-" + rangeend + ")");
context.write(len , result);

}
			 }
