import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implements custom writable to store city ID and string value of an operation system
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HighBitOSWritable implements WritableComparable<HighBitOSWritable> {
    private int cityID;
    private String operationSystem;

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(cityID);
        output.writeUTF(operationSystem);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        cityID = input.readInt();
        operationSystem = input.readUTF();
    }

    @Override
    public int compareTo(@NonNull HighBitOSWritable other) {
        return Integer.compare(cityID, other.cityID);
    }
}
