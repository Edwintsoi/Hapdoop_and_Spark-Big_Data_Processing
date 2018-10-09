import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.Hashtable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

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

            while ((line = br.readLine()) != null) {// part of medalist.csv
                context.getCounter(CustomCounters.NUM_MEDALIST).increment(1);

                String[] lines = line.split(",");
                
                if (lines.length == 11)
                    medalistInfo.put(line[1].toLowerCase(), line[7].toLowerCase()); 
            }
            br.close();
        } catch (IOException e1) {
        }
        super.setup(context);
    }


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] lines = value.toString().split(";");

        if( lines.length == 4){    
            String AL = lines[2];

            if( ( box.length() > 0 ) & (box.length() <= 140)){ 
                for (String keyMedalistName : medalist.keySet()) {
                    if (AL.tolowercase().contains(keyMedalistName)){
                        medalist.set(MedalistName);
                        context.write(medalist, one);
                    }
                   
                }
            }
        }
    }
