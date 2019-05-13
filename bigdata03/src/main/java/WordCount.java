
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();
                if (token.length() < 3)
                    continue;
                word.set(token);

                String count = itr.nextToken();
                one = new IntWritable(Integer.valueOf(count));
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        Comparator<WordCountReducer> comp = new CountComparator();
        PriorityQueue<WordCountReducer> top100WordsQ = new PriorityQueue<>(100, comp);


        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values)
                sum += val.get();
            WordCountReducer wc = new WordCountReducer();
            wc.setWord(key.toString());
            wc.setCount(sum);
            top100WordsQ.add(wc);
            if (top100WordsQ.size() > 100)
                top100WordsQ.poll();

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
            IntWritable count = new IntWritable();
            List<WordCountReducer> result = new ArrayList<>();
            while (top100WordsQ.size() > 0) {
                result.add(0, top100WordsQ.poll());
            }
            int i = 0;
            while (result.size() > i) {
                WordCountReducer wc = result.get(i);
                org.apache.hadoop.io.Text text = new Text(wc.getWord());
                count = new IntWritable(wc.getCount());
                context.write(text, count);
                i++;
            }
        }
    }
    static class WordCountReducer {
        String word;
        Integer count;

        public void setWord(String word1) {
            word = word1;
        }

        public void setCount(Integer count1) {
            count = count1;
        }

        public String getWord() {
            return word;
        }

        public Integer getCount() {
            return count;
        }
    }

    static class CountComparator implements Comparator<WordCountReducer> {
        @Override
        public int compare(WordCountReducer w1, WordCountReducer w2) {
            return w1.getCount() - w2.getCount();
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
