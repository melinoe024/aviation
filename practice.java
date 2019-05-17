import java.util.Calendar;
import java.sql.Timestamp;
import java.util.Date;
import java.text.SimpleDateFormat;

public class practice {

	public static void main(String[] args) {
		String str1 = "17:55:00";
		String str2 = "18:00:00";
		try{
			SimpleDateFormat dateFormat = new SimpleDateFormat("hh:mm:ss");
			Date parsedDate1 = dateFormat.parse(str1);
			Date parsedDate2 = dateFormat.parse(str2);
			//Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
			System.out.println(parsedDate2.getTime() - parsedDate1.getTime());
		} catch (Exception e) {
			
		}
		
		

	}

}
