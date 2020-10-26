import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class FlattenExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        // xd, el vato ya no usa los headers
        PCollection<String> pc1 = pipeline.apply(TextIO.read().from("meh-path/input_1.csv"));
        PCollection<String> pc2 = pipeline.apply(TextIO.read().from("meh-path/input_2.csv"));
        PCollection<String> pc3 = pipeline.apply(TextIO.read().from("meh-path/input_3.csv"));

        PCollectionList<String> pCollectionList = PCollectionList.of(pc1).and(pc2).and(pc3);
        PCollection<String> mergeCollections = pCollectionList.apply(Flatten.pCollections());

        mergeCollections.apply(TextIO
                .write()
                .to("meh/output.csv")
                .withHeader("Id,Name,Last Name,City")
                .withNumShards(1)
                .withSuffix(".csv"));

        pipeline.run();
    }
}
