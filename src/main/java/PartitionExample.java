import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

class PartitionCity implements Partition.PartitionFn<String> {

    @Override
    public int partitionFor(String elem, int numPartitions) {
        String[] arr = elem.split(",");
        if (arr[3].equals("Los Angeles"))
            return 0;
        else if (arr[3].equals("Phoenix"))
            return 1;
        else
            return 2; // para New York
    }
}

public class PartitionExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> users = pipeline.apply(TextIO.read().from("dummy-path/users.csv"));
        // partition example, partition by city
        PCollectionList<String> usersPartition = users.apply(Partition.of(3, new PartitionCity()));

        PCollection<String> pc0 = usersPartition.get(0);
        PCollection<String> pc1 = usersPartition.get(1);
        PCollection<String> pc2 = usersPartition.get(2);

        pc0.apply(TextIO.write().to("dummy-path/pc0.csv").withNumShards(1).withSuffix(".csv"));
        pc1.apply(TextIO.write().to("dummy-path/pc1.csv").withNumShards(1).withSuffix(".csv"));
        pc2.apply(TextIO.write().to("dummy-path/pc2.csv").withNumShards(1).withSuffix(".csv"));

        pipeline.run();
    }
}
