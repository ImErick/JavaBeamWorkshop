import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

class MyFilter implements SerializableFunction<String, Boolean> {

    // basicamente vamos hacer el mismo filtro del ejercicio anterior pero ahora con Filter en lugar de ParDo
    @Override
    public Boolean apply(String input) {
        return input.contains("Los Angeles");
    }
}

public class FilterExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollection = pipeline.apply(TextIO.read().from("meh-path/input.csv"));

        // using Filter
        PCollection<String> output = pCollection.apply(Filter.by(new MyFilter()));
        output.apply(TextIO
                .write()
                .to("meh-path/output.csv")
                .withHeader("Id,Name,Last Name,City")
                .withNumShards(1)
                .withSuffix(".csv"));

        pipeline.run();
    }
}
