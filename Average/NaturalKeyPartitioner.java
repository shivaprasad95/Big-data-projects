import org.apache.hadoop.mapreduce.Partitioner;

public class NaturalKeyPartitioner extends Partitioner<CompositeKey, Average.TextArrayWritable> {

    @Override
    public int getPartition(CompositeKey compositeKey, Average.TextArrayWritable textArrayWritable, int numPartitions) {
        return Math.abs(compositeKey.userId.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}