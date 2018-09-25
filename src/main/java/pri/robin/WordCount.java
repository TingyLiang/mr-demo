package pri.robin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class WordCount {
    /**
     * 此处的泛型参数，需要根据实际情况指定，以便在map阶段得到合适的输入和输出
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable one = new IntWritable(1);
        private Text word = new Text();


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
        }

        /**
         * context它是mapper的一个内部类，简单的说顶级接口是为了在map或是reduce任务中跟踪task的状态，
         * 很自然的MapContext就是记录了map执行的上下文，在mapper类中，这个context可以存储一些job conf的信息，
         * 比如job运行时参数等，我们可以在map函数中处理这个信息，这也是Hadoop中参数传递中一个很经典的例子，
         * 同时context作为了map和reduce执行中各个函数的一个桥梁，这个设计和Java web中的session对象、application对象很相似
         * 简单的说context对象保存了作业运行的上下文信息，比如：作业配置信息、InputSplit信息、任务ID等
         * 我们这里最直观的就是主要用到context的write方法。说白了，context起到的是连接map和reduce的桥梁。起到上下文的作用！
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //The tokenizer uses the default delimiter set, which is " \t\n\r": the space character, the tab character, the newline character, the carriage-return character
            // 对于wordCount来说，此处的key是行好，value是一行的字符串
            // 将Text类型的value转化成字符串类型
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
            }
            context.write(word, one);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);
        }
    }

    /**
     * 同map阶段相同，泛型参数需要根据业务需求进行确定，以便得到合适的输出
     */
    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable item : values) {
                //此处可能经过本地combine进行数据合并，因此不能单纯进行+1操作
                sum += item.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //加载配置文件
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
            System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        List<String> otherArgs = new ArrayList<String>();
        for (int i = 0; i < remainingArgs.length; ++i) {
            if ("-skip".equals(remainingArgs[i])) {
//                job.addCacheFile(new Path(remainingArgs[++i]).toUri());
                job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
            } else {
                otherArgs.add(remainingArgs[i]);
            }
        }
        FileInputFormat.addInputPath(job, new Path(otherArgs.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs.get(1)));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
