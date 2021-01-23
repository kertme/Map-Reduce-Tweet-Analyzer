package mostLikes;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> {

    private TreeMap<Integer, String> treeMap;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        treeMap = new TreeMap<Integer, String>();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Format per tweet is id;user;fullname;url;timestamp;replies;likes;retweets;text
        String tweets = value.toString();
        if (StringUtils.ordinalIndexOf(tweets,";",8)>-1)
        {
            // index starts from 1
            int userStartIndex = StringUtils.ordinalIndexOf(tweets,";",1) + 1;
            int userFinishIndex = StringUtils.ordinalIndexOf(tweets, ";", 2);
            int likesStartIndex = StringUtils.ordinalIndexOf(tweets, ";", 6) + 1;
            int likesFinishIndex = StringUtils.ordinalIndexOf(tweets, ";", 7);

            String user = tweets.substring(userStartIndex, userFinishIndex);
            String likes = tweets.substring(likesStartIndex, likesFinishIndex);

            Integer likeNum;
            try {
                likeNum = new Integer(likes);
            } catch (NumberFormatException e) { // for the first line
                likeNum = -1;
            }

            // # of likes is key
            treeMap.put(likeNum, user);

            // top 10
            if (treeMap.size() > 10)
                treeMap.remove(treeMap.firstKey());
        }
    }

    @Override
    public void cleanup(Context context) throws IOException,
            InterruptedException
    {
        for (Map.Entry<Integer, String> entry : treeMap.entrySet())
        {
            Integer likes = entry.getKey();
            String user = entry.getValue();
            context.write(new Text(user), new IntWritable(likes));
        }
    }

}



