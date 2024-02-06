import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
class Element implements Writable {
    int tag;
    int index;
    double value;

    Element() {
        tag = 0;
        index = 0;
        value = 0.0;
    }

    Element(int tag, int index, double value) {
        this.tag = tag;
        this.index = index;
        this.value = value;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tag = in.readInt();
        index = in.readInt();
        value = in.readDouble();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(tag);
        out.writeInt(index);
        out.writeDouble(value);
    }
}

class Pair implements WritableComparable<Pair> {

    int i;
    int j;

    Pair() {
        i = 0;
        j = 0;
    }

    Pair(int i, int j) {
        this.i = i;
        this.j = j;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }

    @Override
    public int compareTo(Pair p) {
        int compareValue = Integer.compare(i, p.i);

        if (compareValue != 0) {
            return compareValue;
        } else {
            return Integer.compare(j, p.j);
        }
    }

    public String toString() {
        return (i + "," + j);
    }

}

public class Multiply{
    public static class MappingM extends Mapper<Object, Text, IntWritable, Element> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int i = s.nextInt();
            IntWritable j = new IntWritable(s.nextInt());
            double eValue = s.nextDouble();

            context.write(j, new Element(0, i, eValue));
        }
    }

    public static class MappingN extends Mapper<Object,Text,IntWritable,Element> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            IntWritable i = new IntWritable(s.nextInt());
            int j = s.nextInt();
            double eValue = s.nextDouble();
            context.write(i, new Element(1,j, eValue));
        }
    }

    public static class ReducerMult extends Reducer<IntWritable,Element, Pair, DoubleWritable> {

        @Override
        public void reduce(IntWritable key, Iterable<Element> values, Context context) throws IOException, InterruptedException {
            Vector<Element> M = new Vector<>();
            Vector<Element> N = new Vector<>();

            Configuration conf = context.getConfiguration();

            for(Element element : values) {
                Element temp = ReflectionUtils.newInstance(Element.class, conf);
                ReflectionUtils.copy(conf, element, temp);

                if (temp.tag == 0) {
                    M.add(temp);
                } else if(temp.tag == 1) {
                    N.add(temp);
                }
            }

            for(int i=0;i<M.size();i++) {
                for(int j=0;j<N.size();j++) {
                    Pair p = new Pair(M.get(i).index,N.get(j).index);
                    context.write(p, new DoubleWritable(M.get(i).value * N.get(j).value));
                }
            }
        }
    }

    public static class MapAdd extends Mapper<Object, Text, Pair, DoubleWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            Pair p = new Pair(s.nextInt(),s.nextInt());
            context.write(p, new DoubleWritable(s.nextDouble()));
        }
    }

    public static class ReducerAdd extends Reducer<Pair, DoubleWritable, Pair, DoubleWritable> {
        @Override
        public void reduce(Pair key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            for(DoubleWritable value : values) {
                sum += value.get();
            }
            context.write(key, new DoubleWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance();

        final Configuration conf = job.getConfiguration();
        conf.set("mapreduce.textoutputformat.separator",",");
        conf.set("mapreduce.output.textoutputformat.separator",",");
        conf.set("mapreduce.output.key.field.separator",",");
        conf.set("mapred.textoutputformat.separatorText",",");

        job.setJobName("FirstMapReduce");
        job.setJarByClass(Multiply.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MappingM.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MappingN.class);
        job.setReducerClass(ReducerMult.class);

        job.setOutputKeyClass(Pair.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Element.class);

        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.waitForCompletion(true);

        Job job2 = Job.getInstance();
        final Configuration conf2 = job2.getConfiguration();
        conf2.set("mapreduce.textoutputformat.separator",",");
        conf2.set("mapreduce.output.textoutputformat.separator",",");
        conf2.set("mapreduce.output.key.field.separator",",");
        conf2.set("mapred.textoutputformat.separatorText",",");

        job2.setJobName("SecondMapReduce");
        job2.setJarByClass(Multiply.class);

        job2.setMapperClass(MapAdd.class);
        job2.setReducerClass(ReducerAdd.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job2, new Path(args[2]));
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));

        job2.waitForCompletion(true);
    }
}
