import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Implements custom writable to store number of impressions and string value of an operation system
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HighBitOSWritable implements Writable {
    private int highBitPriceImpressions;
    private String operationSystem;

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(highBitPriceImpressions);
        output.writeUTF(operationSystem);
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        highBitPriceImpressions = input.readInt();
        operationSystem = input.readUTF();
    }
}
