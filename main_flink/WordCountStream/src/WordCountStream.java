import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountStream {

    public static void main(String[] args) throws Exception 
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        String path = "/home/sayak/A.csv";
        TextInputFormat inputFormat = new TextInputFormat(new Path(path));
        inputFormat.setCharsetName("UTF-8");
        DataStreamSource<String> ds = env
        		.readFile(inputFormat, path, FileProcessingMode.PROCESS_ONCE, 60000l, BasicTypeInfo.STRING_TYPE_INFO);
        
        DataStream<Tuple2<String, Integer>> dataStream = ds
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .sum(1);
        if (params.has("output")) 
            dataStream.writeAsCsv(params.get("output"));
        dataStream.print();

        env.execute("Window WordCount");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> 
    {
    	static final long serialVersionUID = 1L;
    	@Override
    	public void flatMap(String value, Collector<Tuple2<String, Integer>> out) 
    	{
    		// normalize and split the line
    		String[] tokens = value.toLowerCase().split("\\W+");
    		// emit the pairs
    		for (String token : tokens) 
    			if (token.length() > 0) 
    				out.collect(new Tuple2<>(token, 1));
    	}
    }

}