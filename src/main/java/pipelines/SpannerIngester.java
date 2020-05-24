package pipelines;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
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
 * An example that shows how to template a Spanner ingester. The ingester loads records in Spanner from an
 * external file. The template can be reused from one table to another thanks to the external schema.
 *
 * <p>New Concepts:
 *
 * <pre>
 *   4. Extend Record class to add Primary Key and Transactional behaviour
 *   5. Create Mutations for Spanner
 *   6. Write into Spanner using SpannerIO
 * </pre>
 */
public class SpannerIngester {
    // The delimiter used to split elements from external schema
    private static final String SCHEMA_DELIMITER = "\\|";
    // The delimiter used to split fields from raw records
    private static final String FIELDS_DELIMITER = ";";
    // The string used to indicate that record must be deleted, in this case "D"
    private static final String DELETE_RECORD_FLAG = "D";
    // The maximum number of mutations per batch
    private static final Integer MAXIMUM_NUMBER_MUTATIONS = 10_000;


    /**
     * Class used to store a row from the input table together with the Key and Transaction mode that Spanner requires.
     *
     * The KeyRecord contains the fields that constitute the Primary Key that Spanner uses for transactionality.
     * The TransactionMode informs whether the record must be inserted, updated or deleted.
     *
     * <p>Concept 4: Now, the Record class stores Key and TransactionMode, helping to dealing with Spanner. The KeyRecord
     * and TransactionMode are built through the AdditionalInfo that the schema provides.
     * The KeyRecord is a set of fields that indicates the uniqueness of the record. Spanner relies on this Key to
     * add, overwrite or remove the record from the table. The transactional operation to carry out depends on the
     * TransactionMode stored; typically, the records must be inserted "I" or removed "D" from the table.
     */
    @DefaultCoder(AvroCoder.class)
    static class Record {

        Map<String, Object> Fields;
        Key KeyRecord;
        String TransactionMode;

        private static Object parseValue(String value, String type){

            switch (type) {
                case "INT64":
                    return Long.parseLong(value);
                case "FLOAT64":
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
            KeyRecord = Key.newBuilder().build();
            TransactionMode = "";
        }

        /**
         * Initialises a new Record.
         *
         * @parma schema The schema of the table.
         * @param values The elements of a single row from the table.
         */
        Record(Map<Integer, Map<String, String>> schema, String[] values){

            Fields = new HashMap<>();
            Key.Builder keyBuilder = Key.newBuilder();

            int counter = schema.keySet().size();

            for (int Idx=0; Idx < counter; Idx++){
                Map<String, String> fieldType = schema.get(Idx);

                Object parsedValue = parseValue(values[Idx], fieldType.get("Type"));

                Fields.put(fieldType.get("FieldName"), parsedValue);

                // Add keys
                if (fieldType.get("AdditionalInfo").equals("KEY")){
                    keyBuilder.appendObject(parsedValue);
                }

                // Add transaction column
                if (fieldType.get("AdditionalInfo").equals("TRANSACTION")) {
                    TransactionMode = (String) parsedValue;
                }
            }

            KeyRecord = keyBuilder.build();

        }
    }

    /**
     * The function adds a new value for a field name (column) to the Mutation.WriteBuilder. In this example, just
     * INT64, FLOAT64, DATE and STRING are used. If more types are required, they can be added.
     *
     * @param mutationWriter the Mutation.WriteBuilder to fill with the field names and values of the table
     * @param entry          the Map containing a field name and the corresponding value
     * @return               the Mutation.WriteBuilder after adding a new field
     */
    private static Mutation.WriteBuilder addMutationwithCorespondingType(
            Mutation.WriteBuilder mutationWriter, Map.Entry<String, Object> entry) {

        if (entry.getValue() instanceof Long){
            mutationWriter = mutationWriter.set(entry.getKey()).to((Long) entry.getValue());
        } else if (entry.getValue() instanceof Double){
            mutationWriter = mutationWriter.set(entry.getKey()).to((Double) entry.getValue());
        } else if (entry.getValue() instanceof String || entry.getValue() instanceof LocalDate){
            mutationWriter = mutationWriter.set(entry.getKey()).to((String) entry.getValue());
        }

        return mutationWriter;
    }

    /**
     * Custom DoFn that takes a Record and creates a Mutation for Spanner.
     *
     * <p>Concept #5: In Spanner, a Mutation is a single modification that must be applied to a table. In this example,
     * two operations (types of mutations) can take place, depending on the TransactionMode of the Record:
     *    * Insert or update
     *    * Delete
     * If the record must be inserted or updated, the resulting Mutation.WriteBuilder must be filled with all fields of
     * the Fields attribute that the Record contains. This is achieved using the addMutationwithCorrespondingType function.
     * If the record must be deleted, the KeyRecord attribute is used to build a Mutation that tells to Spanner
     * which existing row of the table must be deleted based on the primary Key.
     */
    public static class GenerateMutations extends DoFn<Record, Mutation>{
        @ProcessElement
        public void processElement(ProcessContext c){
            Record record = c.element();
            Options options = c.getPipelineOptions().as(Options.class);
            String spannerTable = options.getSpannerTable().get();
            Mutation mutation;

            if (!record.TransactionMode.equals(DELETE_RECORD_FLAG)) {

                Mutation.WriteBuilder mutationWriter = Mutation.newInsertOrUpdateBuilder(spannerTable);

                for (Map.Entry<String, Object> entry : record.Fields.entrySet()) {
                    mutationWriter = addMutationwithCorespondingType(mutationWriter, entry);
                }
                mutation = mutationWriter.build();

            } else {
                mutation = Mutation.delete(spannerTable, record.KeyRecord);
            }

            c.output(mutation);
        }
    }

    /**
     * Split each row from the input schema and returns a KV element. The key is the field position
     * and the value is a Map containing the field name, type and additional information for the field.
     */
    private static class GenerateSchema extends DoFn<String, KV<Integer, Map<String, String>>> {
        @ProcessElement
        public void ProcessElement(ProcessContext c){
            String line = c.element();
            String[] lineComponents = line.split(SCHEMA_DELIMITER, -1);

            Map<String, String> fieldType= new HashMap<>();
            fieldType.put("FieldName", lineComponents[1]);
            fieldType.put("Type", lineComponents[2]);

            // Add AdditionalInfo regarding Key or Transactional Behaviour
            String additionalInfo;
            try {
                additionalInfo = lineComponents[3];
            } catch (Exception e){
                additionalInfo = "";
            }
            fieldType.put("AdditionalInfo", additionalInfo);

            c.output(KV.of(Integer.parseInt(lineComponents[0]), fieldType));
        }
    }

    /**
     * A function that takes lines from the input table as String and returns custom Records based on the schema provided.
     */
    private static PCollection<Record> generateRecords(
            PCollection<String> rawLines, PCollectionView<Map<Integer, Map<String, String>>> schemaView)
    {
        PCollection<Record> records = rawLines.apply(
                "GenerateRecordsFromLines", ParDo.of(new DoFn<String, Record>() {

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
     */
    private static PCollectionView<Map<Integer, Map<String, String>>> generateSchemaView(
            Pipeline pipeline, Options options){

        return pipeline
                .apply("ReadSchemaLines", TextIO.read().from(options.getSchema()))
                .apply("GenerateSchema", ParDo.of(new GenerateSchema()))
                .apply("SchemaToView", View.asMap());

    }

    /**
     * Options supported by {@link SpannerIngester}.
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

        /* Spanner Instance */
        @Description("Name of Spanner Instance")
        @Required
        ValueProvider<String> getSpannerInstance();

        void setSpannerInstance(ValueProvider<String> value);

        /* Spanner Database */
        @Description("Name of Spanner Database within the Spanner Instance")
        @Required
        ValueProvider<String> getSpannerDatabase();

        void setSpannerDatabase(ValueProvider<String> value);

        /* Spanner Table */
        @Description("Name of Spanner Table within the Spanner Database")
        @Required
        ValueProvider<String> getSpannerTable();

        void setSpannerTable(ValueProvider<String> value);

    }

    private static void runSpannerIngester(Options options){
        Pipeline pipeline = Pipeline.create(options);

        PCollectionView<Map<Integer, Map<String, String>>> schema = generateSchemaView(pipeline, options);

        PCollection<String> rawLines = pipeline
                .apply("ReadLines", TextIO.read().from(options.getInputFilePattern()));

        PCollection<Record> records = generateRecords(rawLines, schema);

        PCollection<Mutation> mutations = records.apply("GenerateMutations", ParDo.of(new GenerateMutations()));


        /**
         * Write into Spanner the PCollection of Mutations using the SpannerIO.write() method
         *
         * <p>Concept #6: SpannerIO allows to create a transformation step of type SpannerIO.Write through the
         * write() method. In this transformation, the Spanner instance and database can be specified through the
         * options pipeline. The mutations are written in batches and here a maximum number of mutations per batch
         * is specified using .withMaxNumMutations(). There are other possibilities regarding batch configuration,
         * like Batch Size Bytes or Max Number of Rows.
         */
        mutations.apply("WriteMutations", SpannerIO.write()
                .withInstanceId(options.getSpannerInstance())
                .withDatabaseId(options.getSpannerDatabase())
                .withMaxNumMutations(MAXIMUM_NUMBER_MUTATIONS));

        pipeline.run();

    }

    public static void main(String[] args){

        Options options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        runSpannerIngester(options);
    }
}
