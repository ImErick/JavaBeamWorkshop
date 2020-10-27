import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.values.PCollection;

public class DistinctExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> repeatedUsers = pipeline.apply(TextIO.read().from("dummy-path/users.csv"));

        // distinct example, check if ALL the row is duplicated(the same), otherwise it's different
        PCollection<String> uniqueUsers = repeatedUsers.apply(Distinct.create());

        uniqueUsers.apply(TextIO.write().to("dummy-path/output.csv").withNumShards(1).withSuffix(".csv"));

        pipeline.run();
    }
}
