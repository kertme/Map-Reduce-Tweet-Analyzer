package mostRetweets;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TweetReducer extends Reducer<Text, IntWritable, IntWritable, Text> {

    private TreeMap<Integer, String> treeMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        treeMap = new TreeMap<Integer, String>();
    }

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        String user = key.toString();
        int sum = 0;
        for (IntWritable value : values) {
            sum = sum + value.get();
        }

        treeMap.put(sum, user);

        // top 10
        if (treeMap.size() > 10)
            treeMap.remove(treeMap.firstKey());

        //context.write(key, new IntWritable(sum));
    }

    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException
    {
        for (Map.Entry<Integer, String> entry : treeMap.entrySet())
        {
            Integer retweets = entry.getKey();
            String user = entry.getValue();
            context.write(new IntWritable(retweets), new Text(user));
        }
    }
}