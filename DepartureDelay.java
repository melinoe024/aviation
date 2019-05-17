//import org.apache.flink.api.common.functions.FilterFunction;
import java.util.Arrays;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.util.Collector;

import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import java.util.Calendar;
import java.sql.Timestamp;
import java.util.Date;
import java.text.SimpleDateFormat;

public class DepartureDelay {
	
	public static void main(String[] args) throws Exception {
	    // get output file command line parameter - or use "top_rated_users.txt" as default
	    final ParameterTool params = ParameterTool.fromArgs(args);
	    String output_filepath = params.get("output", "/home/mkim8797/data3404/departure_delay.txt");

	    // obtain handle to execution environment
	    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

	    // define a 'ratings' data set from the example AuctionDB.comments file with user_id_to and rating
	    DataSet<Tuple3<String, String, String>> flight_times =
	    env.readCsvFile("/home/mkim8797/data3404/data/assignment_data_files/ontimeperformance_flights_small.csv")
	      .includeFields("010000010100")
	      .ignoreFirstLine()
	      .ignoreInvalidLines()
	      .types(String.class,String.class,String.class);
	    
	   
	    DataSet<Tuple3<String, String, String>> delayed =
	  	      flight_times.filter(new FilterFunction<Tuple3<String, String, String>>() {
	  	                            public boolean filter(Tuple3<String, String,String> entry) { 
	  	                            	if (entry.f2.equals("")) return false;
	  	                            	return Integer.parseInt(String.join("",entry.f1.split(":"))) < Integer.parseInt(String.join("",entry.f2.split(":"))); }
	  	                     });
	    
	    
	    DataSet<Tuple3<String, String, String>> airlines =
	    env.readCsvFile("/home/mkim8797/data3404/data/assignment_data_files/ontimeperformance_airlines.csv")
	      .includeFields("111")
	      .ignoreFirstLine()
	      .ignoreInvalidLines()
	      .types(String.class,String.class,String.class);
	    
	    
	    DataSet<Tuple3<String, String, String>> us_airlines =
		  	      airlines.filter(new FilterFunction<Tuple3<String, String, String>>() {
		  	                            public boolean filter(Tuple3<String, String,String> entry) { 
		  	                            	return entry.f2.equals("United States"); }
		  	                     });
	    
	    DataSet<Tuple5<String, String, String, String,String>> joinresult = 
	    		   delayed.join(us_airlines).where(0).equalTo(0).projectFirst(0,1,2).projectSecond(1,2);
	    //DataSet<Tuple2<Tuple3<String, String, String>, Tuple3<String, String, String>>> joinresult =
	    //	       delayed.join(us_airlines).where(0).equalTo(0).project(0,1);
	    
	    DataSet<Tuple4<String,Integer,String,String>> time_diff = joinresult.project(0,1,3,4);
	    

	
	    
	    
	    time_diff
	    .writeAsText(output_filepath, WriteMode.OVERWRITE);
	    env.execute("Executing");
		
	}

}