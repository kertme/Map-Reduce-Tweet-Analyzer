package timeStamp;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text data = new Text();
    private final IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Format per tweet is id;user;fullname;url;timestamp;replies;likes;retweets;text
        String tweets = value.toString();
        if (StringUtils.ordinalIndexOf(tweets,";",8)>-1){

            int startIndex = StringUtils.ordinalIndexOf(tweets,";",4) + 1;
            int finishIndex = StringUtils.ordinalIndexOf(tweets, ";", 5);

            //split by ',' and take the first element (2012-07-27, 20:48:57, BST)
//            String tweet_date = tweets.substring(startIndex,finishIndex).split(", ")[0];
            String tweet_date = tweets.substring(startIndex,finishIndex);
            data.set(tweet_date);
            context.write(data, one);
        }
    }

}



