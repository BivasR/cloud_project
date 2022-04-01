import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
public class WordCount {

   // *************************************************************************
   // PROGRAM
   // *************************************************************************
   public static void main(String[] args) throws Exception {
      final ParameterTool params = ParameterTool.fromArgs(args);
      // set up the execution environment
      final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
      // make parameters available in the web interface
      env.getConfig().setGlobalJobParameters(params);
      // get input data
      DataSet<String> text = env.readTextFile(params.get("input"));
      DataSet<Tuple2<String, Double>> counts =
      // split up the lines in pairs (2-tuples) containing: (word,1)
      text.flatMap(new Tokenizer())
      // group by the tuple field "0" and sum up tuple field "1"
      .groupBy(0)
      .sum(1);
      // emit result
      if (params.has("output")) {
         counts.writeAsCsv(params.get("output"), "\n", " ");
         // execute program
         env.execute("WordCount Example");
      } else {
         System.out.println("Printing result to stdout. Use --output to specify output path.");
         counts.print();
      }
   }
   
   // *************************************************************************
   // USER FUNCTIONS
   // *************************************************************************
   public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Double>> {
      public void flatMap(String value, Collector<Tuple2<String, Double>> out) {
         // normalize and split the line
         String[] tokens = value.toLowerCase().split(",");
         if (!tokens[2].equals("high")) 
         {
        	 String date=tokens[0];
        	 Double high= new Double(tokens[2]);
        	 // emit the pairs
         out.collect(new Tuple2<>(date, high));
         }
         /*
         for (String token : tokens) {
            if (token.length() > 0) {
               out.collect(new Tuple2<>(token, 1));
            }
         }*/
      }
   }
}