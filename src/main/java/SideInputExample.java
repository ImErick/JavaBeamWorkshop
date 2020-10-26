import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;

public class SideInputExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        // SideInput example
        PCollection<KV<String, String>> pReturn = pipeline
                .apply(TextIO.read().from("dummy-path/returns.csv"))
                .apply(ParDo.of(new DoFn<String, KV<String, String>>() {
                    @ProcessElement
                    public void process(ProcessContext processContext) {
                        String[] arr = processContext.element().split(","); // que cagadero es esta funciooon
                        processContext.output(KV.of(arr[0], arr[1]));
                    }
                }));

        PCollectionView<Map<String, String>> pMap = pReturn.apply(View.asMap());

        PCollection<String> pCustomerOrder = pipeline.apply(TextIO.read().from("dummy-path/customer_orders.csv"));
        pCustomerOrder.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void process(ProcessContext processContext) {
                Map<String, String> pSideView = processContext.sideInput(pMap);
                String[] arr = processContext.element().split(",");
                String customerName = pSideView.get(arr[0]);

                if (customerName ==  null) System.out.println(processContext.element());
            }
        }).withSideInputs(pMap));


        pipeline.run();
    }
}
