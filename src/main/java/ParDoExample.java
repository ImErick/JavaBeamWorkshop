import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

class CustomerFilter extends DoFn<String, String > {
    @ProcessElement
    public void processElement(ProcessContext context) {
        String line = context.element();
        String[] arr = line.split(",");

        if(arr[3].equals("Los Angeles")) context.output(line);
    }
}

public class ParDoExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> customerList = pipeline.apply(TextIO.read().from("meh-path/input.csv"));

        // using ParDo
        PCollection<String> output = customerList.apply(ParDo.of(new CustomerFilter()));
        output.apply(TextIO
                .write()
                .to("meh-path/output.csv")
                .withHeader("Id,Name,Last Name,City")
                .withNumShards(1)
                .withSuffix(".csv"));

        pipeline.run().waitUntilFinish();
    }
}
