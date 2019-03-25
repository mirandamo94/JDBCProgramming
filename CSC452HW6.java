import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CSC452HW6 {

	public static void main(String[] args) {
		
		System.out.println("********************************************");
		// 1) Declare variables for Table movie
		//and parse into array by splitting at "::"
		//1::Toy Story (1995)::Animation|Children's|Comedy
		String line = "";
		String movieTitle = "";
		String movieId = "";
		String movieYear = "";
		String categories = "";
		
		String moviesTable = "Movies";
		String categoryTable = "Categories";
		
		ArrayList<String> id = new ArrayList<String>();
		ArrayList<String> year = new ArrayList<String>();
		ArrayList<String> title = new ArrayList<String>();
		ArrayList<String> category = new ArrayList<String>();
		String[] movies = null;
		String[] movieIds = null;
		String[] movieTitles = null;
		String[] movieYears = null;
		
		String moviesCSV = "src/movies.dat";
		System.out.println("Parsing MoviesCSV...");
		try (BufferedReader reader = new BufferedReader(new FileReader(moviesCSV))) {
			while ((line = reader.readLine()) != null) {
				if (line != null) {
					 movies = line.split("::");
					 
					 //movieIds
					 movieId=movies[0];
					 id.add(movieId);
					 
					 //movie titles
					 movieTitles = movies[1].split("\\(\\d{4}\\)");
					 title.add(movieTitles[0]);
					 
					 //pattern to extract movie years
					 Matcher m = Pattern.compile("\\(([^)]+)\\)").matcher(movies[1]);
				     while(m.find()) {
				    	 movieYear = m.group(1);
				    	 year.add(movieYear);    
				     }
					//Categories: System.out.print(movies[2]);
				     categories = movies[2].replaceAll("\\|"," "); 
				     category.add(categories);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("Parsing MoviesCSV was a success! \n");

		// 2) Declare variables for Table Users 
		//and parse into array by splitting at "::"
		//The User table (from users.dat) will contain columns for 
		//UserID, Gender, AgeCode, Occupation, and Zipcode.
		//EXAMPLE- 1::F::1::10::48067

		String userTable = "Users";
		String[] users = null;
		String userCSV = "src/users.dat";
		ArrayList<String> userId = new ArrayList<String>();
		ArrayList<String> gender = new ArrayList<String>();
		ArrayList<String> ageCode = new ArrayList<String>();
		ArrayList<String> occupation = new ArrayList<String>();
		ArrayList<String> zipcode = new ArrayList<String>();
		System.out.println("Parsing UsersCSV...");
		try (BufferedReader reader = new BufferedReader(new FileReader(userCSV))) {
			while ((line = reader.readLine()) != null) {
				if (line != null) {
					users = line.split("::");
					userId.add(users[0]);
					gender.add(users[1]);
					ageCode.add(users[2]);
					occupation.add(users[3]);
					zipcode.add(users[4]);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Parsing UsersCSV was a success! \n");

		// 3) Declare variables for Table ratings 
		//and parse into array by splitting at "::"
		//The Ratings table will contain columns for 
		//UserID, MovieID, Rating, and Timestamp
		String ratingsTable = "Ratings";
		String[] ratings = null;
		String ratingsCSV = "src/ratings.dat";
		ArrayList<String> userIdForRatings = new ArrayList<String>();
		ArrayList<String> movieIdForRatings = new ArrayList<String>();
		ArrayList<String> rating = new ArrayList<String>();
		ArrayList<String> timestamp = new ArrayList<String>();
		
		System.out.println("Parsing RatingsCSV...");
		
		try (BufferedReader reader = new BufferedReader(new FileReader(userCSV))) {
			while ((line = reader.readLine()) != null) {
				if (line != null) {
					ratings = line.split("::");
					userIdForRatings.add(ratings[0]);
					movieIdForRatings.add(ratings[1]);
					rating.add(ratings[2]);
					timestamp.add(ratings[3]);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Parsing RatingsCSV was a success!");
		
		
		String occupationTable = "Occupation";
		String ageTable = "Age";

		//**********************************
				//**********************************
				// Establish database connection.
				//**********************************
				//**********************************
						Statement st = null;
						//Statement stmt = null;
						try {
							Class.forName("oracle.jdbc.OracleDriver");
						} catch (ClassNotFoundException e) {
							e.printStackTrace();
						}// end catch
						Connection c = null;
						try {
							System.out.println("********************************************");
							System.out.println("Connecting to the database...");
							 c = DriverManager.getConnection("jdbc:oracle:thin:@acadoradbprd01.dpu.depaul.edu:1521:ACADPRD0", 
									 "XMO",
									"cdm1360075");
							System.out.println("Oracle Database has been successfully connected! ");
							st = c.createStatement();
						} catch (SQLException e) {
							System.out.println(e);
							System.exit(1);
						} //end catch

		// Dropping all the pre-existing tables and get ready to create new
		// ones...
		// If they do not exist, there is no need to do anything
		System.out.println("********************************************");
		System.out.println("Dropping pre-existing tables...");
		try {
			String dropQuery = "DROP TABLE " + moviesTable;
			st.executeUpdate(dropQuery);
			dropQuery = "DROP TABLE " + userTable;
			st.executeUpdate(dropQuery);
			dropQuery = "DROP TABLE " + ratingsTable;
			st.executeUpdate(dropQuery);
			dropQuery = "DROP TABLE " + categoryTable;
			st.executeUpdate(dropQuery);
			dropQuery = "DROP TABLE " + occupationTable;
			st.executeUpdate(dropQuery);
			dropQuery = "DROP TABLE " + ageTable;
			st.executeUpdate(dropQuery);
		} catch (SQLException e) {

		}
		System.out.println("Tables have successfully been dropped!");

		/*
		 * create and populate new tables: 
		 * movies users ratings categories 
		 */
		try {

			/*
			 * The Movies table (from movies.dat) will contain four columns:
			 * MovieId, MovieTitle, Year, and Category. You will need to
			 * separate the year from the title and store the year in its own
			 * column. You will also need to separate the repeating groups in
			 * Category. A separate table is preferred for the Movie-Category
			 * relationship.
			 */
			System.out.println("********************************************");
			System.out.println("Creating new table " + moviesTable + "...");
			String tableQuery = "CREATE TABLE " + moviesTable 
					+ "  (MovieId VARCHAR2(5) NOT NULL PRIMARY KEY,"
					+ "  MovieTitle VARCHAR2(128), " 
					+ "  Year VARCHAR(128), " 
					+ "  Category VARCHAR2(128))";
			st.executeUpdate(tableQuery);
			System.out.println("Table has been successfully created");
			
			System.out.println("Populating table " + moviesTable + "...");
			PreparedStatement ppstmt = c.prepareStatement("INSERT INTO MOVIES" 
					+ " (MovieId, MovieTitle, Year, Category) "
					+ " VALUES(?, ?, ?, ?)");
			try{
				
					for (int i=0; i< id.size(); i++){
						ppstmt .setString(1, id.get(i)); 
						ppstmt .setString(2, title.get(i)); 
						ppstmt .setString(3, year.get(i));
						ppstmt .setString(4, category.get(i));
						ppstmt .executeUpdate();
					}
					System.out.println ("Table " + moviesTable + " has been populated.");
				
				
			}catch(SQLException e){
				e.printStackTrace();
			}
		
			
			/*
			 * The User table (from users.dat) will contain columns for UserID,
			 * Gender, AgeCode, Occupation, and Zipcode.
			 */
			System.out.println("\nCreating new table " + userTable + "...");
			tableQuery = "CREATE TABLE " + userTable 
					+ "  (UserId VARCHAR2(5) NOT NULL PRIMARY KEY,"
					+ "   Gender VARCHAR(1)," 
					+ "   AgeCode VARCHAR(8)," 
					+ "   Occupation VARCHAR2(2),"
					+ "   Zipcode VARCHAR(10))";
			st.executeUpdate(tableQuery);
			System.out.println("Table has been successfully created");
			
			System.out.println("Populating table " + userTable + "...");
			try{
				//The User table (from users.dat) will contain columns for UserID, Gender, AgeCode, Occupation, and Zipcode
					PreparedStatement pstmt = c.prepareStatement("INSERT INTO USERS" 
							+ " (UserId, Gender, AgeCode, Occupation, Zipcode) "
							+ " VALUES(?, ?, ?, ?, ?)");
					for (int i=0; i< userId.size(); i++){
						pstmt .setString(1, userId.get(i)); 
						pstmt .setString(2, gender.get(i)); 
						pstmt .setString(3, ageCode.get(i));
						pstmt .setString(4, occupation.get(i));
						pstmt .setString(5, zipcode.get(i));
						pstmt .executeUpdate();
					}
					System.out.println ("Table " + userTable + " has been populated.");
				
			}catch(SQLException e){
				e.printStackTrace();
			}
			

			/*
			 * All ratings are contained in the file "ratings.dat" and are in
			 * the following format: UserID::MovieID::Rating::Timestamp -
			 * UserIDs range between 1 and 6040 - MovieIDs range between 1 and
			 * 3952 - Ratings are made on a 5-star scale (whole-star ratings
			 * only) - Timestamp is represented in seconds since the epoch as
			 * returned by time(2) - Each user has at least 20 ratings
			 */
			System.out.println("\nCreating new table " + ratingsTable + "...");
			tableQuery = "  CREATE TABLE " + ratingsTable 
					+ "  (UserId VARCHAR2(5) NOT NULL PRIMARY KEY,"
					+ "   MovieId VARCHAR(5) NOT NULL," 
					+ "   Rating VARCHAR(2)," 
					+ "   Timestamps VARCHAR(10))";
			st.executeUpdate(tableQuery);
			System.out.println("Table has been successfully created");
			
			System.out.println("Populating " + ratingsTable + "...");
			try{
				PreparedStatement p = c.prepareStatement("INSERT INTO RATINGS" 
						+ " (UserId, MovieId, Rating, Timestamps) "
						+ " VALUES(?, ?, ?, ?)");
				for (int i=0; i< rating.size(); i++){
						p.setString(1, userIdForRatings.get(i)); 
						p.setString(2,movieIdForRatings.get(i)); 
						p.setString(3,rating.get(i));
						p.setString(4,timestamp.get(i));
						p.executeUpdate();
				}
				System.out.println ("Table " + ratingsTable + " has been populated.");
			}catch(SQLException e){
				e.printStackTrace();
			}  
	        

			System.out.println("\nCreating new table " + categoryTable + "...");
			tableQuery = "  CREATE TABLE " + categoryTable 
					+ "  (MovieId VARCHAR2(5) NOT NULL PRIMARY KEY,"
					+ "  Category VARCHAR(128))";
			st.executeUpdate(tableQuery);
			System.out.println("Table has been successfully created");
			String[] cate = new String[category.size()];
			cate = category.toArray(cate);
			System.out.println("Populating table " + categoryTable + "...");
			try{
				String sql = "INSERT INTO CATEGORIES"+" (MovieId, Category) "+" VALUES(?, ?)" ;
				PreparedStatement ps =  c.prepareStatement(sql);
				for (int i=0; i< id.size(); i++){	
					//System.out.println(cate[i]);
					ps.setString(1, id.get(i));
					ps.setString(2, cate[i]);
					ps.executeUpdate();
				}
				System.out.println ("Table " + categoryTable + " has been populated.");
			}catch(SQLException e){
				e.printStackTrace();
			}
			
			System.out.println("\nCreating new table " + occupationTable + "...");
			tableQuery = "  CREATE TABLE " + occupationTable 
					+ "  (OccupationId VARCHAR2(56) NOT NULL PRIMARY KEY,"
					+ "   OccDescription VARCHAR(128))";
			st.executeUpdate(tableQuery);
			System.out.println("Table has been successfully created");
			System.out.println("Populating table " + occupationTable + "...");
			try{
				String sql = "INSERT INTO OCCUPATION"+" (OccupationId, OccDescription) "+" VALUES(?, ?)" ;
				PreparedStatement ps =  c.prepareStatement(sql);
				String[] des = {"other or not specified",
								"academic/educator",
								"artist",
								"clerical/admin",
								"college/grad student",
								"customer service",
								"doctor/health care",
								"executive/managerial",
							    "farmer",
							    "homemaker",
							    "K-12 student",
							    "lawyer",
							    "programmer",
							    "retired",
							    "sales/marketing",
							    "scientist",
							    "self-employed",
							    "technician/engineer",
							    "tradesman/craftsman",
							    "unemployed",
							    "writer"};
				for (int i=0; i< des.length; i++){
					ps.setInt(1, i);
					ps.setString(2, des[i]);
					ps.executeUpdate();
				}
				System.out.println ("Table " + occupationTable + " has been populated.");
			}catch(SQLException e){
				e.printStackTrace();
			}
			
			System.out.println("\nCreating new table " + ageTable + "...");
			tableQuery = "  CREATE TABLE " + ageTable 
					+ "  (AgeId VARCHAR2(56) NOT NULL PRIMARY KEY,"
					+ "   AgeDescription VARCHAR(8))";
			st.executeUpdate(tableQuery);
			System.out.println("Table has been successfully created");
			System.out.println("Populating table " + occupationTable + "...");
			try{
				String sql = "INSERT INTO AGE"+" (AgeId, AgeDescription) "+" VALUES(?, ?)" ;
				PreparedStatement ps =  c.prepareStatement(sql);
				String[] aId = {"1","18","25","35","45","50","56"};
				String[] ageD = {"Under 18","18-24","25-34","35-44","45-49","50-55","56+"};
				for (int i=0; i< aId.length; i++){
					ps.setString(1, aId[i]);
					ps.setString(2, ageD[i]);
					ps.executeUpdate();
				}
				System.out.println ("Table " + ageTable + " has been populated.");
			}catch(SQLException e){
				e.printStackTrace();
			}
			
		} catch (SQLException e) {
			System.out.println("SQL ERROR: " + e);
		}	
		System.out.println("********************************************");
		
		//using select queries to print out tables
		System.out.println("Printing out table: "+moviesTable+"...");
		try{
		ResultSet resultSet = st.executeQuery("SELECT * FROM MOVIES");
		ResultSetMetaData rsmd = resultSet.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		System.out.println("MovieId  MovieTitle                Year  Category          ");
		System.out.println("___________________________________________________________");
		while (resultSet.next()) {
		    for (int i = 1; i <= columnsNumber; i++) {
		        if (i > 1) System.out.print(",  ");
		        String columnValue = resultSet.getString(i);
		        System.out.print(columnValue + " " );
		    }
		    System.out.println("");
		}
		}catch(SQLException e){
			e.printStackTrace();
		}
		//UserID, Gender, AgeCode, Occupation, and Zipcode
		System.out.println("**********************************************");
		System.out.println("Printing out table: "+userTable+"...");
		try{
		ResultSet resultSet = st.executeQuery("SELECT * FROM USERS");
		ResultSetMetaData rsmd = resultSet.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		System.out.println("UserID Gender AgeCode Occupation Zipcode          ");
		System.out.println("___________________________________________________________");
		while (resultSet.next()) {
		    for (int i = 1; i <= columnsNumber; i++) {
		        if (i > 1) System.out.print("|  ");
		        String columnValue = resultSet.getString(i);
		        System.out.print(columnValue + " " );
		    }
		    System.out.println("");
		}
		}catch(SQLException e){
			e.printStackTrace();
		}
		//UserID, MovieID, Rating, and Timestamp
		System.out.println("**********************************************");
		System.out.println("Printing out table: "+ratingsTable+"...");
		try{
		ResultSet resultSet = st.executeQuery("SELECT * FROM RATINGS");
		ResultSetMetaData rsmd = resultSet.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		System.out.println("UserID MovieID Rating Timestamp          ");
		System.out.println("___________________________________________________________");
		while (resultSet.next()) {
		    for (int i = 1; i <= columnsNumber; i++) {
		        if (i > 1) System.out.print("|  ");
		        String columnValue = resultSet.getString(i);
		        System.out.print(columnValue + " " );
		    }
		    System.out.println("");
		}
		}catch(SQLException e){
			e.printStackTrace();
		}
		//Category Table
		System.out.println("**********************************************");
		System.out.println("Printing out table: "+categoryTable+"...");
		try{
		ResultSet resultSet = st.executeQuery("SELECT * FROM CATEGORIES");
		ResultSetMetaData rsmd = resultSet.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		System.out.println("MovieID Categories         ");
		System.out.println("___________________________________________________________");
		while (resultSet.next()) {
		    for (int i = 1; i <= columnsNumber; i++) {
		        if (i > 1) System.out.print("|  ");
		        String columnValue = resultSet.getString(i);
		        System.out.print(columnValue + " " );
		    }
		    System.out.println("");
		}
		}catch(SQLException e){
			e.printStackTrace();
		}
		//print out age table
		System.out.println("**********************************************");
		System.out.println("Printing out table: "+ageTable+"...");
		try{
		ResultSet resultSet = st.executeQuery("SELECT * FROM AGE");
		ResultSetMetaData rsmd = resultSet.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		System.out.println("AgeID Age Description         ");
		System.out.println("___________________________________________________________");
		while (resultSet.next()) {
		    for (int i = 1; i <= columnsNumber; i++) {
		        if (i > 1) System.out.print("|  ");
		        String columnValue = resultSet.getString(i);
		        System.out.print(columnValue + " " );
		    }
		    System.out.println("");
		}
		}catch(SQLException e){
			e.printStackTrace();
		}
		//print out occupation Table
		System.out.println("**********************************************");
		System.out.println("Printing out table: "+occupationTable+"...");
		try{
		ResultSet resultSet = st.executeQuery("SELECT * FROM occupation");
		ResultSetMetaData rsmd = resultSet.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		System.out.println("OccID  Occupation Description        ");
		System.out.println("___________________________________________________________");
		while (resultSet.next()) {
		    for (int i = 1; i <= columnsNumber; i++) {
		        if (i > 1) System.out.print("|  ");
		        String columnValue = resultSet.getString(i);
		        System.out.print(columnValue + " " );
		    }
		    System.out.println("");
		}
		}catch(SQLException e){
			e.printStackTrace();
		}
		//Run A Customized Query
		//Get everyone age above 35's strictly Horror Movie ratings
		//Grouped by UserID, and ordered by rating scores from low to high
		System.out.println("**********************************************");
		
		System.out.println("listing top rated movies by average rating, "
				+ "without the movies that aren't rated by 5 or more users");
		try{
		ResultSet resultSet = st.executeQuery(" SELECT MovieID, avg_rating"+ 
												"FROM "+
												"(SELECT MovieID,avg(rating) AS avg_rating,"+
												"count(rating) AS rating_count"+
												"FROM ratings"+ 
												"GROUP BY MovieID)" + 
												"WHERE rating_count >= 5" +
												"ORDER BY avg_rating DESC;");
		ResultSetMetaData rsmd = resultSet.getMetaData();
		int columnsNumber = rsmd.getColumnCount();
		System.out.println("MovieId  Average Rating         ");
		System.out.println("___________________________________________________________");
		while (resultSet.next()) {
		    for (int i = 1; i <= columnsNumber; i++) {
		        if (i > 1) System.out.print("|  ");
		        String columnValue = resultSet.getString(i);
		        System.out.print(columnValue + " " );
		    }
		    System.out.println("");
		}
		}catch(SQLException e){
			e.printStackTrace();
		}
	}
	}
