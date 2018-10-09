import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.Hashtable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private Text medalist = new Text();
    private Hashtable<String, String> medalistInfo;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        medalistInfo = new Hashtable<String, String>();

        URI fileUri = context.getCacheFiles()[0];
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open(new Path(fileUri));

        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String line = null;
        try {
            br.readLine();

            while ((line = br.readLine()) != null) {
                context.getCounter(CustomCounters.NUM_MEDALIST).increment(1);

                String[] lines = line.split(",");
               
                if (lines.length == 11)
                    medalistInfo.put(line[1], line[7]);
            }
            br.close();
        } catch (IOException e1) {
        }
        super.setup(context);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] lines = value.toString().split(";");

        if( lines.length == 4){   
            box = lines[2];
            epoch_timeMilliseconds = lines[0];

            if( ( box.length() > 0 ) & (box.length() <= 140)){ 
                try{
                    entryDate = new Date ( Long.parseLong(epoch_timeMilliseconds));
                    iHour24 = entryDate.toInstant().atZone( zoneRIO ).getHour();
			                

                    if ( iHour24 == HOUR){
			                   A = Pattern.compile(REGEXPATTERN);
                         B = A.matcher( box.toString().toLowerCase() );
                         while (B.find()) {
                            hashtag.set( B.group(1) );
                            context.write(hashtag, one);			}
                    }
                }catch(Exception e){
                   
                    return;
                }
            }
        }
    }
}
