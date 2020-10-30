import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

import java.util.stream.Collectors;

public class SQLExample {
    final static String HEADER = "userId,orderId,product,Id,Amount";
    final static Schema schema = Schema
            .builder()
            .addStringField("userId")
            .addStringField("orderId")
            .addStringField("productId")
            .addDoubleField("Amount")
            .build();

    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> input = pipeline.apply(TextIO.read().from("dummy-path/users.csv"));

        // convertiendo strings to rows
        PCollection<Row> rows = input.apply(ParDo.of(new StringToRow())).setRowSchema(schema);
        // sql "SELECT userId, Count(userId) FROM PCOLLECTION GROUP BY userId" -- count query
        PCollection<Row> sqlResult = rows.apply(SqlTransform.query("SELECT * FROM PCOLLECTION"));
        // regresar rows a strings
        PCollection<String> finalOutput = sqlResult.apply(ParDo.of(new RowToString()));

        pipeline.run();
    }

    public static class StringToRow extends DoFn<String, Row> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (!c.element().equalsIgnoreCase(HEADER)) {
                String[] arr = c.element().split(",");
                Row row = Row.withSchema(schema).addValues(arr[0], arr[1], arr[2], Double.valueOf(arr[3])).build();
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
                    .collect(Collectors.joining(","));

            c.output(output);
        }

    }
}
