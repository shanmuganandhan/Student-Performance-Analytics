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

public class AttendanceAnalysis {

    public static class AttendanceMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        private final static Text keyText = new Text("Average Attendance");
        private DoubleWritable attendance = new DoubleWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            if (line.startsWith("id,")) {
                return;
            }

            String[] fields = line.split(",");

            if (fields.length >= 5) {
                try {
                    attendance.set(Double.parseDouble(fields[4]));
                    context.write(keyText, attendance);
                } catch (NumberFormatException e) {
                    // Ignore invalid attendance values
                }
            }
        }
    }

    public static class AttendanceReducer
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

        Job job = Job.getInstance(conf, "Attendance Analysis");

        job.setJarByClass(AttendanceAnalysis.class);

        job.setMapperClass(AttendanceMapper.class);
        job.setReducerClass(AttendanceReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
