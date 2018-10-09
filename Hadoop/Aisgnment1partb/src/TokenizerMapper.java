import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.text.*;
import java.time.*;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>

{
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();
    public void map(Object key, Text line, Context context) throws IOException, InterruptedException{
      String[] lines = line.toString().split(";");
      try{
      if(lines.length== 4){
long hours = Long.parseLong(line[0]);

    Date date = new Date(hours);
    DateFormat format = new SimpleDateFormat("HH");
    format.setTimeZone(TimeZone.getTimeZone("GMT-3"));
    String formatted = format.format(date);
    data.set(formatted);
context.write(data,one);
}}
catch(Exception e){
  return;
}
        }
}
