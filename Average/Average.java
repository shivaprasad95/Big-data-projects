import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


class Friends implements Comparable<Friends>{
    int count;
    String key;
    String friends_list;

    Friends(String key, int count, String list) {
        this.key = key;
        this.count = count;
        this.friends_list = list;
    }

    @Override
    public int compareTo(Friends o) {
        return (this.count - o.count);
    }
}


public class Average {
    //private static PriorityQueue<Friends> queue = new PriorityQueue<>(10);
    static int cc = 1;


    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(int[] values) {
            super(IntWritable.class);
            IntWritable[] vals = new IntWritable[values.length];
            for (int i = 0; i < values.length; i++) {
                vals[i] = new IntWritable(values[i]);
            }
            set(vals);

        }

        public int[] getArray() {
            int[] arr = new int[this.get().length];
            for (int i = 0; i < this.get().length; i++) {
                arr[i] = Integer.valueOf(this.get()[i].toString());
            }
            return arr;
        }
    }

    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }

        public String getStrings() {
            String strings = "";
            for (int i = 0; i < this.get().length; i++) {
                strings = strings + "~"+ this.get()[i].toString();
            }
            return strings;
        }

        public int getSize() {
            return this.get().length;
        }

    }


    public static class Map1
            extends Mapper<LongWritable, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);


        private Text word = new Text(); // type of output key



        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] lines = value.toString().split("\n");
            for(String str: lines) {
                String[] kvstring = str.split("\\s+");

                String k = "";
                String v = "";

                if(kvstring.length == 2) {
                    k = kvstring[0];
                    v = kvstring[1];
                } else {
                    continue;
                }

                String[] values = v.split(",");
                String[] valuesWithSize = new String[values.length+1];
                String op = "";

                for(int i=0;i<values.length;i++){
                    op+= values[i];
                    op+="~";
                }
//                int[] tvaluesInt = new int[values.length];
//                String[] details = new String[values.length];
//
//                for(int i=0;i<values.length;i++) {
//                    tvaluesInt[i] = Integer.valueOf(values[i]);
//                    details[i] = hashMap.get(tvaluesInt[i]);
//                }


                //String[] joinedValues = new String[values.length];

//
//                float average = sum/(float)dobs.length;
//                CompositeKey ck = new CompositeKey(k,Float.toString(average));


                context.write(new Text(k),new Text(op));




            }
        }
    }

    public static class Reduce1
            extends Reducer<Text,Text,CompositeKey,Text> {
        HashMap<String,String> hashMap1 = new HashMap<>();



        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String[] confInput =  conf.get("USERDATA").split("~");
            for(String line : confInput) {
                String[] words = line.split("_");
                String uId = words[0];
                String address = words[1];
                String dob = words[2];
                //System.err.println("uid: "+ uId  + " address: " + address + " dob: " + dob);


                if(!hashMap1.containsKey(uId)) {
                    hashMap1.put(uId,address+ "_" + dob);
                    //System.err.println(uId + " found");
                }
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String friendsList = "";

            for(Text value: values) {
                friendsList = value.toString();
            }

            String[] friendsArray = friendsList.split("~");
            String[] userDetails = new String[friendsArray.length];
            String[] dobs = new String[friendsArray.length];
            String[] ages = new String[friendsArray.length];

            for(int i=0;i<friendsArray.length;i++) {
                String[] comb = hashMap1.get(friendsArray[i]).split("_");
                dobs[i] = comb[1];
            }
            String address = hashMap1.get(key.toString()).split("_")[0];
            float sum = 0;
            for(int i=0;i<dobs.length;i++) {
                try {
                    Date date1=new SimpleDateFormat("MM/dd/yyyy").parse(dobs[i]);
                    Date today = new Date();
                    long diff = today.getTime() - date1.getTime();
                    float diffDays =  ((float)diff / (float)(24 * 60 * 60 * 1000));
                    float age = (float)diffDays/(float) 365;
                    ages[i] = Float.toString(age);
                    sum += age;

                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

            float average = sum/(float)dobs.length;

            CompositeKey ck = new CompositeKey(key.toString(),Float.toString(average));

            context.write(ck,new Text(key.toString()+", "+ address+ ", " + average ));

        }
    }

    public static class Map2 //Sort and partition
            extends Mapper<LongWritable, Text, CompositeKey, Text>{

        private final static IntWritable one = new IntWritable(1);


        private Text word = new Text(); // type of output key



        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                String values[] = value.toString().split(",");
                String uid = values[0];
                String avg = values[2];

                CompositeKey ck = new CompositeKey(uid,avg);
                context.write(ck,value);

        }
    }

    public static class Reduce2
            extends Reducer<CompositeKey,Text,CompositeKey,Text> {

        public void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text t: values) {
                context.write(key, t);
            }

        }
    }

    public static class Map3 //Sort and partition
            extends Mapper<LongWritable, Text, IntWritable, Text>{

        private final static IntWritable one = new IntWritable(1);


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(one,value);
        }
    }

    public static class Reduce3
            extends Reducer<IntWritable,Text,Text,Text> {
        Queue<String> queue = new LinkedList<>();
        final int qsize = 15;

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text t: values) {
                String line = t.toString();
                if(queue.size() <15) {
                    queue.add(line);
                } else {
                    queue.remove();
                    queue.add(line);
                }
                //context.write(new Text(t.toString()), new Text(""));
            }
            for(String str: queue) {
                context.write(new Text(str), new Text(""));
            }

        }
    }








    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 4) {
            System.err.println(otherArgs[0]);
            System.err.println(otherArgs.length);
            System.err.println("Usage: Average <LiveJournalAdj in hdfs> <userdata in local system> <output>");
            System.exit(2);
        }
        //String op = readFile("userdata.txt", Charset.defaultCharset());

        List<String> lines = Files.readAllLines(Paths.get(otherArgs[2]), Charset.defaultCharset());

        //List<String> lines = Files.readAllLines(Paths.get("/Users/shivaprasadreddybitla/Desktop/userdata.txt"), Charset.defaultCharset());

        String value = "";
        for(String line: lines ){
            //System.err.println(line);
            String[] words = line.split(",");
            //System.err.println(words[0]+words[1]+words[2]+words[3]+words[4]);
            String key = words[0];
            value += words[0]+"_"+words[3]+"_"+words[9]+"~";
        }
        //value = value.substring(0,value.length()-1);
        //System.err.println(value);
        conf1.set("USERDATA",value);



        Job job1 = new Job(conf1, "MapReduce phase 1");
        job1.setJarByClass(Average.class);
        job1.setMapperClass(Map1.class);
        job1.setReducerClass(Reduce1.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);




        // uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);

        // set output key type
        job1.setOutputKeyClass(CompositeKey.class);
        // set output value type
        //job.setOutputValueClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
        //job.setMapOutputValueClass(ArrayWritable.class);

        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job1, new Path(otherArgs[1]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job1, new Path(otherArgs[3]+"/temp"));
        //Wait till job completion
        if(job1.waitForCompletion(true)) {
            Configuration conf2 = new Configuration();
            Job job2 = new Job(conf2, "Map Reduce phase 2");
            job2.setJarByClass(Average.class);
            job2.setMapperClass(Map2.class);
            job2.setReducerClass(Reduce2.class);
            job2.setMapOutputKeyClass(CompositeKey.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(CompositeKey.class);
            job2.setOutputValueClass(Text.class);
            job2.setPartitionerClass(NaturalKeyPartitioner.class);
            job2.setSortComparatorClass(FullKeyComparator.class);
            job2.setGroupingComparatorClass(NaturalKeyComparator.class);
            FileInputFormat.addInputPath(job2, new Path(otherArgs[3]+ "/temp/part-r-00000"));
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]+"/temp1"));
            if(job2.waitForCompletion(true)) {
                Configuration conf3 = new Configuration();
                Job job3 = new Job(conf3, "Map Reduce phase 3");
                job3.setJarByClass(Average.class);
                job3.setMapperClass(Map3.class);
                job3.setReducerClass(Reduce3.class);
                job3.setMapOutputKeyClass(IntWritable.class);
                job3.setMapOutputValueClass(Text.class);
                job3.setOutputKeyClass(Text.class);
                job3.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job3, new Path(otherArgs[3]+ "/temp1/part-r-00000"));
                FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3]+"/final"));
                System.err.println("Check output at <output>/final");
                System.exit(job3.waitForCompletion(true) ? 0 : 1);
            }
        }

    }

//    static String readFile(String path, Charset encoding)
//            throws IOException
//    {
//        byte[] encoded = Files.readAllBytes(Paths.get(path));
//        return new String(encoded, encoding);
//    }
}