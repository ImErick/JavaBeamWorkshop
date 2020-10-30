import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

import java.util.stream.Collectors;

public class SQLJoinExample {
    private final static String order_header = "userId,orderId,productId,Amount";
    private final static Schema order_schema = Schema
            .builder()
            .addStringField("userId")
            .addStringField("orderId")
            .addStringField("productId")
            .addDoubleField("Amount")
            .build();

    private final static String user_header = "userId,name";
    private final static Schema user_schema = Schema
            .builder()
            .addStringField("userId")
            .addStringField("name")
            .build();

    //private final static String order_user_header = "userId,name,orderId,productId,Amount";
    private final static Schema order_user_schema = Schema
            .builder()
            .addStringField("userId")
            .addStringField("name")
            .addStringField("orderId")
            .addStringField("productId")
            .addDoubleField("Amount")
            .build();

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> orders = pipeline.apply(TextIO.read().from("dummy-path/orders.csv"));
        PCollection<String> users = pipeline.apply(TextIO.read().from("dummy-path/users.csv"));
        // convert string to rows
        PCollection<Row> orderRows = orders.apply(ParDo.of(new StringToOrderRow())).setRowSchema(order_schema);
        PCollection<Row> userRows = orders.apply(ParDo.of(new StringToUserRow())).setRowSchema(user_schema);
        // query
        PCollection<Row> sqlInput = PCollectionTuple
                .of(new TupleTag<>("orders"), orderRows)
                .and(new TupleTag<>("users"), userRows)
                .apply(SqlTransform.query("SELECT o.*, u.name FROM orders o INNER JOIN users u ON o.userId = u.userId"));
        // de regreso de row a String
        PCollection<String> output = sqlInput.apply(ParDo.of(new RowToString()));
        output.apply(TextIO.write().to("").withSuffix("csv").withNumShards(1));

        pipeline.run();
    }

    public static class StringToOrderRow extends DoFn<String, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (!c.element().equalsIgnoreCase(order_header)) {
                String[] arr = c.element().split(",");
                Row row = Row.withSchema(order_schema).addValues(arr[0], arr[1], arr[2], Double.valueOf(arr[3])).build();
                c.output(row);
            }
        }
    }

    public static class StringToUserRow extends DoFn<String, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (!c.element().equalsIgnoreCase(user_header)) {
                String[] arr = c.element().split(",");
                Row row = Row.withSchema(user_schema).addValues(arr[0], arr[1]).build();
                c.output(row);
            }
        }
    }

    public static class RowToString extends DoFn<Row, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String output = c
                    .element()
                    .getValues()
                    .stream()
                    .map(Object::toString)
                    .collect(Collectors.joining(",")); // left join: filter(entity -> entity != null)

            c.output(output);
        }
    }
}
