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
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
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
    private static final String SCHEMA_DELIMITER = "\\s+";
    // The delimiter used to split fields from raw records
    private static final String FIELDS_DELIMITER = ";";


    @DefaultCoder(AvroCoder.class)
    static class Record {

        HashMap<String, Object> Fields;

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

    private static PCollection<Record> generateRecords(
            PCollection<String> rawLines, PCollectionView<Map<Integer, Map<String, String>>> schemaView)
    {
        PCollection<Record> records = rawLines.apply(
                "GenerateDictRowFromLines", ParDo.of(new DoFn<String, Record>() {

                    @ProcessElement
                    public void ProcessElement(ProcessContext c) {

                        String line = c.element();
                        String[] values = line.split(FIELDS_DELIMITER);

                        Map<Integer, Map<String, String>> schema = c.sideInput(schemaView);

                        Record record = new Record(schema, values);

                        c.output(record);
                    }

                }).withSideInputs(schemaView));

        return records;
    }

    private static PCollectionView<Map<Integer, Map<String, String>>> generateSchemaView(
            Pipeline pipeline, Options options){

        return pipeline
                .apply("ReadSchemaLines", TextIO.read().from(options.getSchema()))
                .apply("GenerateSchema", ParDo.of(new GenerateSchema()))
                .apply("SchemaToView", View.asMap());

    }

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
