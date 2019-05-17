import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.util.Collector;

import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;

public class TopThreeCessna {

	public static void main(String[] args) throws Exception {
		// get output file command line parameter - or use "top_rated_users.txt" as default
	    final ParameterTool params = ParameterTool.fromArgs(args);
	    String output_filepath = params.get("output", "/home/mkim8797/data3404/top_three_cessna.txt");
	    
	    // obtain handle to execution environment
	    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
	    
	    // define a 'models' data set from the aircrafts file with manufacturer and model
	    DataSet<Tuple2<String, String>> models =
	      env.readCsvFile("/home/mkim8797/data3404/data/assignment_data_files/ontimeperformance_aircrafts.csv")
	      .includeFields("001010000")
	      .ignoreFirstLine()
	      .ignoreInvalidLines()
	      .types(String.class,String.class);
	    
	    // filter for manufacturer == cessna
	    // to count how many ratings we have per user, we extend each entry with a constant '1' which we later sum up over
	    DataSet<Tuple2<String, String>> cessna =
	      models.filter(new FilterFunction<Tuple2<String, String>>() {
	                            public boolean filter(Tuple2<String,String> entry) { return entry.f0.equals("CESSNA"); }
	                     });
	      //.flatMap(new ModelMapper());
	
	    // group by user_id, count how many 5-ratings per user; print most popular users first
	    cessna
	      //.groupBy(1)       //group according to user id
	      //.sum(2)           // count how many ratings per user
	      //.sortPartition(1, Order.DESCENDING)
	      //.print();// commented out as would trigger execution directly and prints on screen!
	      .writeAsText(output_filepath, WriteMode.OVERWRITE);

	    // execute the FLink job
	    env.execute("Executing sample1 program");
	    // alternatively: get execution plan
	    // System.out.println(env.getExecutionPlan());

	    // wait 20secs at end to give us time to inspect ApplicationMAster's WebGUI
	    Thread.sleep(20000);

	}

	  // private FlatMapper class that maps a <userid,rating> tuple to <userid,1>
	  private static class ModelMapper implements FlatMapFunction<Tuple2<String,String>, Tuple3<String,String,Integer>> {
	    @Override
	    public void flatMap( Tuple2<String,String> input_tuple, Collector<Tuple3<String,String,Integer>> out) {
	      out.collect(new Tuple3<String,String,Integer>(input_tuple.f0,input_tuple.f1,1));
	    }
	  }

}
