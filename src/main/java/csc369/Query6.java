package csc369;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Query6 {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String[] sa = value.toString().split(" ");
            // number of bytes for current request
            IntWritable bytes = new IntWritable();
            bytes.set(Integer.parseInt(sa[9]));

            String temp = value.toString().split(":")[0];
            String day_month_year[] = temp.split("\\[")[1].split("/");

            StringBuilder sb = new StringBuilder();
            sb.append(day_month_year[0]);
            sb.append(" ");
            sb.append(day_month_year[1]);
            sb.append(" ");
            sb.append(day_month_year[2]);

            Text calendarDay = new Text();
            calendarDay.set(sb.toString());
            context.write(calendarDay, bytes);
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text code, Iterable<IntWritable> intOne,
                              Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = intOne.iterator();

            while (itr.hasNext()){
                sum  += itr.next().get();
            }
            result.set(sum);
            context.write(code, result);
        }
    }
}
