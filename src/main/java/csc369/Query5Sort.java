package csc369;

import java.text.DateFormatSymbols;
import java.io.IOException;
import java.time.Month;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Query5Sort {

    public static final Class OUTPUT_KEY_CLASS = Text.class;
    public static final Class OUTPUT_VALUE_CLASS = IntWritable.class;

    public static class MapperImpl extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {

            Map<String, String> monthToNumber = new HashMap<String, String>();
            monthToNumber.put("Jan", "1");
            monthToNumber.put("Feb", "2");
            monthToNumber.put("Mar", "3");
            monthToNumber.put("Apr", "4");
            monthToNumber.put("May", "5");
            monthToNumber.put("Jun", "6");
            monthToNumber.put("Jul", "7");
            monthToNumber.put("Aug", "8");
            monthToNumber.put("Sept", "9");
            monthToNumber.put("Oct", "10");
            monthToNumber.put("Nov", "11");
            monthToNumber.put("Dec", "12");

            String[] month_year = value.toString().split("\t");
            String month = month_year[0].split("/")[0];
            String year = month_year[0].split("/")[1];

            if (monthToNumber.containsKey(month)) {
                StringBuilder sb = new StringBuilder();
                sb.append(monthToNumber.get(month));
                sb.append("/");
                sb.append(year);
                Text date = new Text();
                date.set(sb.toString());
                IntWritable bytes = new IntWritable();
                bytes.set(Integer.parseInt(month_year[1]));
                context.write(date, bytes);
            }
        }
    }

    public static class ReducerImpl extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text code, Iterable<IntWritable> intOne,
                              Context context) throws IOException, InterruptedException {
            String date = code.toString();
            String[] month_year = date.split("/");

            Iterator<IntWritable> itr = intOne.iterator();

            Map<Integer, String> numberToMonth = new HashMap<Integer, String>();
            numberToMonth.put(1, "Jan");
            numberToMonth.put(2, "Feb");
            numberToMonth.put(3, "Mar");
            numberToMonth.put(4, "Apr");
            numberToMonth.put(5, "May");
            numberToMonth.put(6, "Jun");
            numberToMonth.put(7, "Jul");
            numberToMonth.put(8, "Aug");
            numberToMonth.put(9, "Sept");
            numberToMonth.put(10, "Oct");
            numberToMonth.put(11, "Nov");
            numberToMonth.put(12, "Dec");

            Integer monthNumber = Integer.parseInt(month_year[0]);

            if (numberToMonth.containsKey(monthNumber)) {
                StringBuilder sb = new StringBuilder();
                sb.append(numberToMonth.get(monthNumber));
                sb.append(" ");
                sb.append(month_year[1]);

                Text monthAndYear = new Text();
                monthAndYear.set(sb.toString());

                result.set(itr.next().get());

                context.write(monthAndYear, result);

            }
        }
    }
}
