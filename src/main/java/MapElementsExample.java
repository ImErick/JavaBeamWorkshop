import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

class User extends SimpleFunction<String, String> {
    @Override
    public String apply(String input) {
        String[] arr = input.split(",");

        String sid = arr[0];
        String uid = arr[1];
        String uname = arr[2];
        String vid = arr[3];
        String duration = arr[4];
        String startTime = arr[5];
        String sex = arr[6];

        // return to change the number in sex column(1||2) to letter(M||F)
        return (sex.equals("1")) ? sid + "," + uid + "," + uname + "," + vid + "," + duration + "," + startTime + "," + "M" :
                sid + "," + uid + "," + uname + "," + vid + "," + duration + "," + startTime + "," + "F";
    }
}

public class MapElementsExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> customers = pipeline.apply(TextIO.read().from("/Users/erickgarcia/Downloads/user.csv"));

        //using Simple Function
        PCollection<String> output = customers.apply(MapElements.via(new User())); // SimpleFunction
        output.apply(TextIO
                .write()
                .to("/Users/erickgarcia/Desktop/resultado.csv")
                .withNumShards(1)
                .withSuffix(".csv"));

        pipeline.run();
    }
}
