import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class CountExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCount = pipeline.apply(TextIO.read().from("dummy-path/users.csv"));

        // Count transformation, NO MAMES el cagadero para hacer un count
        PCollection<Long> result = pCount.apply(Count.globally());
        result.apply(ParDo.of(new DoFn<Long, Void>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                System.out.println(processContext.element());
            }
        }));
    }
}
