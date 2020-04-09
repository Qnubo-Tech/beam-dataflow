package pipelines;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

/**
 * An example that shows how to use an external file as a side input and define a custom class
 * for processing
 *
 * <p>New Concepts:
 *
 * <pre>
 *   1. Create a PCollectionView from external files
 *   2. Use the PCollectionView as a side input
 *   3. Create a custom class for PCollection processing
 * </pre>
 */
public class SideInputSchema {
    // The delimiter used to split elements from external schema
    private static final String SCHEMA_DELIMITER = "\\|";
    // The delimiter used to split fields from raw records
    private static final String FIELDS_DELIMITER = ";";


    /**
     * Class used to store a row from the input table.
     *
     * The Fields attribute is a map between the field name and the value casted to the corresponding type.
     *
     * <p>Concept 3: The custom class is defined to handle each row of the input table. Within
     * this class user-defined transformations can be applied or additional information can be stored
     * for later processing. The @DefaultCoder annotation together with the AvroCoder.class are
     * required for serialization.
     */
    @DefaultCoder(AvroCoder.class)
    static class Record {

        Map<String, Object> Fields;

        private static Object parseValue(String value, String type){

            switch (type) {
                case "INTEGER":
                    return Long.parseLong(value);
                case "FLOAT":
                    return Double.parseDouble(value);
                case "DATE":
                    return LocalDate.parse(value);
                case "STRING":
                    return value;
                default:
                    throw new IllegalStateException("Unexpected value: " + type);
            }
        }

        Record(){
            Fields =  new HashMap<>();
        }

        /**
         * Initialises a new Record.
         *
         * @parma schema The schema of the table.
         * @param values The elements of a single row from the table.
         */
        Record(Map<Integer, Map<String, String>> schema, String[] values){

            Fields = new HashMap<>();

            int counter = schema.keySet().size();

            for (int Idx=0; Idx < counter; Idx++){
                Map<String, String> fieldType = schema.get(Idx);

                Object parsedValue = parseValue(values[Idx], fieldType.get("Type"));

                Fields.put(fieldType.get("FieldName"), parsedValue);

            }
        }
    }

    /**
     * Split each row from the input schema and returns a KV element. The key is the field position
     * and the value is a Map of field name - type.
     */
    private static class GenerateSchema extends DoFn<String, KV<Integer, Map<String, String>>> {
        @ProcessElement
        public void ProcessElement(ProcessContext c){
            String line = c.element();
            String[] lineComponents = line.split(SCHEMA_DELIMITER, -1);

            Map<String, String> fieldType= new HashMap<>();
            fieldType.put("FieldName", lineComponents[1]);
            fieldType.put("Type", lineComponents[2]);

            c.output(KV.of(Integer.parseInt(lineComponents[0]), fieldType));
        }
    }

    /**
     * A function that takes lines from the input table as String and returns custom Records based on the schema provided.
     *
     * <p>Concept 2: The PCollectionView which contains the schema is used here as a side input.
     * Then, the schemaView becomes an additional input when the DoFn processes a new element from
     * the rawLines PCollection. That means the schema can be accessed each time a new line
     * from the input table is being processed.
     */
    private static PCollection<Record> generateRecords(
            PCollection<String> rawLines, PCollectionView<Map<Integer, Map<String, String>>> schemaView)
    {
        PCollection<Record> records = rawLines.apply(
                "GenerateDictRowFromLines", ParDo.of(new DoFn<String, Record>() {

                    @ProcessElement
                    public void ProcessElement(ProcessContext c) {

                        String line = c.element();
                        String[] values = line.split(FIELDS_DELIMITER);

                        // Get the schema as an additional input
                        Map<Integer, Map<String, String>> schema = c.sideInput(schemaView);

                        // Convert the values of the row processed into a single Record object
                        Record record = new Record(schema, values);

                        c.output(record);
                    }

                }).withSideInputs(schemaView));

        return records;
    }

    /**
     * A function that takes a Pipeline, read the schema rows and convert them to a PCollectionView.
     *
     * <p>Concept #1: The GenerateSchema function takes the raw schema rows as String and generates a KV
     * element for each single row. Then, all KV elements are converted into a Map View.
     * Hence, if the schema file has n rows, the PCollectionView will have a Map of n entries.
     *
     */
    private static PCollectionView<Map<Integer, Map<String, String>>> generateSchemaView(
            Pipeline pipeline, Options options){

        return pipeline
                .apply("ReadSchemaLines", TextIO.read().from(options.getSchema()))
                .apply("GenerateSchema", ParDo.of(new GenerateSchema()))
                .apply("SchemaToView", View.asMap());

    }

    /**
     * Options supported by {@link SideInputSchema}.
     *
     * <p>Inherits standard configuration options.
     */
    public interface Options extends PipelineOptions {

        /* Input File Pattern */
        @Description("Path of the file pattern to read from")
        @Required
        ValueProvider<String> getInputFilePattern();

        void setInputFilePattern(ValueProvider<String> value);

        /* Input Schema */
        @Description("Path of the schema table to process")
        @Required
        ValueProvider<String> getSchema();

        void setSchema(ValueProvider<String> value);

    }

    private static void runSideInputSchema(Options options){
        Pipeline pipeline = Pipeline.create(options);

        PCollectionView<Map<Integer, Map<String, String>>> schema = generateSchemaView(pipeline, options);

        PCollection<String> rawLines = pipeline
                .apply("ReadLines", TextIO.read().from(options.getInputFilePattern()));

        PCollection<Record> records = generateRecords(rawLines, schema);

        pipeline.run();

    }

    public static void main(String[] args){

        Options options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        runSideInputSchema(options);
    }
}
