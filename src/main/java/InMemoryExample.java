import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.List;

public class InMemoryExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<CustomerEntity> pList = pipeline.apply(Create.of(getCustomers()));

        /* la operacion de a contnuacion se tiene que realizar porque TextIO solo te acepta Strings,
        se convierte el DF de CustomerEntity a un DF de Strings
        */
        PCollection<String> customerDataSet = pList.apply(
                MapElements
                        .into(TypeDescriptors.strings())
                        .via(CustomerEntity::getName)); // TODO: ojala responda como se hace para ID y NAME

        customerDataSet.apply(TextIO
                .write()
                .to("/Users/erickgarcia/Downloads/otherTest.csv")
                .withNumShards(1)
                .withSuffix(".csv"));

        pipeline.run();
    }

    private static List<CustomerEntity> getCustomers() {
        CustomerEntity customerEntity_1 = new CustomerEntity("666", "Erick");
        CustomerEntity customerEntity_2 = new CustomerEntity("1", "Monse");

        List<CustomerEntity> list = new ArrayList<>();
        list.add(customerEntity_1);
        list.add(customerEntity_2);

        return list;
    }
}
