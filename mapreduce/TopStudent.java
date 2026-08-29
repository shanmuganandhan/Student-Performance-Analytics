import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopStudent {

    public static class TopStudentMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text name = new Text();
        private IntWritable marks = new IntWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip header
            if (line.startsWith("id,")) {
                return;
            }

            String[] fields = line.split(",");

            if (fields.length >= 4) {
                name.set(fields[1]);
                marks.set(Integer.parseInt(fields[3]));

                context.write(name, marks);
            }
        }
    }

    public static class TopStudentReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Text topStudent = new Text();
        private int highestMarks = -1;

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            for (IntWritable value : values) {
                if (value.get() > highestMarks) {
                    highestMarks = value.get();
                    topStudent.set(key);
                }
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            context.write(topStudent, new IntWritable(highestMarks));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Top Performing Student");

        job.setJarByClass(TopStudent.class);

        job.setMapperClass(TopStudentMapper.class);
        job.setReducerClass(TopStudentReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
