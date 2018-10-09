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
import java.io.IOException;
import org.apache.log4j.Logger;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>

{
    private final IntWritable one = new IntWritable(1);
     StringTokenizer tokenizer = new StringTokenizer(line);
    private Text data = new Text();
    public void map(Object key, Text line, Context context) throws IOException, InterruptedException{
      String[] lines = line.toString().split(";");
      try{
      if(lines.length== 4){
long hours = Long.parseLong(lines[0]);

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

if(token.startsWith("#")){
         word.set(token.toLowerCase());
			}
                    }
                }
