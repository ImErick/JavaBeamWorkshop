import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;

public class InstallTest {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        // de esta manera se crea un DF desde un archivo CSV
        PCollection<String> output =  pipeline.apply(TextIO.read().from("/Users/erickgarcia/Downloads/addresses.csv"));

        // de esta manera se escribe el DF en un path del sistema
        output.apply(TextIO
                .write()
                .to("/Users/erickgarcia/Downloads/test.csv")
                .withNumShards(1) // si no le agregamos esta opcion lo particiona por supongo numero de cores
                .withSuffix(".csv")); // con esto le agrego el formato al archivo de salida

        pipeline.run(); // es importante esta parte porque si no, no corre nara
        System.out.println("done!");
    }
}
