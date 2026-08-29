import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DepartmentAverage {

    public static class DepartmentMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private Text department = new Text();
        private DoubleWritable marks = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("id,")) {
                return;
            }

            String[] fields = line.split(",");

            if (fields.length >= 5) {
                department.set(fields[2]);

                try {
                    marks.set(Double.parseDouble(fields[3]));
                    context.write(department, marks);
                } catch (NumberFormatException e) {
                    // Ignore invalid marks
                }
            }
        }
    }

    public static class DepartmentReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            int count = 0;

            for (DoubleWritable value : values) {
                sum += value.get();
                count++;
            }

            if (count > 0) {
                result.set(sum / count);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Department Wise Average");

        job.setJarByClass(DepartmentAverage.class);

        job.setMapperClass(DepartmentMapper.class);
        job.setReducerClass(DepartmentReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
