import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {
    void setInputFile(String file);
    String getInputFile();

    void setOutputFile(String file);
    String getOutputFile();

    void setExtension(String extension);
    String getExtension();
}

/*
 * Ejemplo del comando de Java para la consola y poder correr el jar parametrizado:
 *
 * java -cp JavaBeamWorkshop-1.0-SNAPSHOT-jar-with-dependencies.jar InstallTest
 * --inputFile="/Users/erickgarcia/Downloads/addresses.csv"
 * --outputFile="/Users/erickgarcia/Desktop/test.csv"
 * --extension=".csv"
 */