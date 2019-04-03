import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable<CompositeKey> {

    public String userId;
    public String averageAge;

    public CompositeKey() {
    }

    public CompositeKey(String userId, String averageAge) {
        super();
        this.set(userId, averageAge);
    }

    public void set(String state, String city) {
        this.userId = (state == null) ? "" : state;
        this.averageAge = (city == null) ? "" : city;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(userId);
        out.writeUTF(averageAge);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        userId = in.readUTF();
        averageAge = in.readUTF();
    }

    @Override
    public int compareTo(CompositeKey o) {
        return Integer.valueOf(this.averageAge) - Integer.valueOf(o.averageAge);

    }

    @Override
    public String toString() {
        //return "uid: " + userId +  " avgAge: " + averageAge;
        return "";
    }
}