import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FullKeyComparator extends WritableComparator {

    public FullKeyComparator() {
        super(CompositeKey.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2) {

        CompositeKey key1 = (CompositeKey) wc1;
        CompositeKey key2 = (CompositeKey) wc2;

        if(Float.valueOf(key1.averageAge) - Float.valueOf(key2.averageAge) > 0) {
            return 1;
        } else if(Float.valueOf(key1.averageAge) - Float.valueOf(key2.averageAge) < 0) {
            return -1;
        } else {
            return 0;
        }

    }
}