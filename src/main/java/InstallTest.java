import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class InstallTest {
    public static void main(String[] args) {
        MyOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        // de esta manera se crea un DF desde un archivo CSV
        PCollection<String> output =  pipeline.apply(TextIO.read().from(options.getInputFile()));

        // de esta manera se escribe el DF en un path del sistema
        output.apply(TextIO
                .write()
                .to(options.getOutputFile())
                .withNumShards(1) // si no le agregamos esta opcion lo particiona por supongo numero de cores
                .withSuffix(options.getExtension())); // con esto le agrego el formato al archivo de salida

        pipeline.run(); // es importante esta parte porque si no, no corre nara
        System.out.println("done!");
    }
}
