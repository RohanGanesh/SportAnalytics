package com.yamltest;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

public class YamlProcesser {
   // JDBC driver name and database URL

   static Logger log = Logger.getLogger(
		   YamlProcesser.class.getName());


/*This is the class which processes the yaml file and inserts records into DB
 * 
 * 
 */
   
   
   /*
    * The function caled from the main class
    */
	public static List<Set<String>> processFile(InputStream input,
			String fileName) {
		log.info("Entering processFile");
		Yaml yaml = new Yaml();
		Map list = (HashMap) yaml.load(input);
		String matchId = processandInsertMatchInfo(input, list);
		List<Set<String>> playersList = processBallInfo(input, matchId, list);
		return playersList;
	}

	
	
	/*
	 * This function is used to process the ballBy ball information and construct Sql to insert into BallInfo table
	 */
	private static List<Set<String>> processBallInfo(InputStream input,
			String matchId, Map list) {
		log.info("Entering processFile");
		Map firstInningsInfo = null;
		Map secondInningsInfo = null;
		List<Set<String>> playersList = new ArrayList<Set<String>>();
		Set<String> batsmanSet = null;
		if (list.size() == 3) {
			if (((ArrayList) list.get(CricketdataConstants.INNINGS)).size() == 2) {
				firstInningsInfo = (HashMap) ((ArrayList) list
						.get(CricketdataConstants.INNINGS)).get(0);
				secondInningsInfo = (HashMap) ((ArrayList) list
						.get(CricketdataConstants.INNINGS)).get(1);
				if (firstInningsInfo != null && firstInningsInfo.size() > 0)
					batsmanSet = processinningsInfo(firstInningsInfo, matchId);
				playersList.add(batsmanSet);
				if (secondInningsInfo != null && secondInningsInfo.size() > 0)
					batsmanSet = processinningsInfo(secondInningsInfo, matchId);
				playersList.add(batsmanSet);
			} else if (((ArrayList) list.get(CricketdataConstants.INNINGS))
					.size() == 1) {
				firstInningsInfo = (HashMap) ((ArrayList) list
						.get(CricketdataConstants.INNINGS)).get(0);
				if (firstInningsInfo != null && firstInningsInfo.size() > 0)
					batsmanSet = processinningsInfo(firstInningsInfo, matchId);
				playersList.add(batsmanSet);
			}
		}
		/*
		 * if (playersList != null) processPlayerInfo(playersList);
		 */

		return playersList;
	}

	/*
	 * 
	 * This function processes the input and constructs sql to be inserted into matchInfo table
	 */
	private static String processandInsertMatchInfo(InputStream input, Map list) {
		// TODO Auto-generated method stub
		log.info("Entering processFile");
		String season;
		String matchId;
		String sql;
		StringBuffer matchInfosql = new StringBuffer();
		List<String> insertQueries = new ArrayList<String>();
		int insertedMatchIdCount;
		String eliminatorResult = "";
		Map matchInfo = (HashMap) list.get(CricketdataConstants.MATCHINFO);
		Date date = (Date) ((ArrayList) matchInfo.get(CricketdataConstants.MATCHDATE)).get(0);
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		Map winBy = (HashMap) (((HashMap) (matchInfo.get(CricketdataConstants.MATCHOUTCOME))).get("by"));
		int year = cal.get(Calendar.YEAR);
		int month = cal.get(Calendar.MONTH)+1;
		int day = cal.get(Calendar.DAY_OF_MONTH);
		StringBuffer insertDate = new StringBuffer();
		insertDate.append(year).append("-").append(month).append("-")
				.append(day);
		season = getSeason(year);
		matchId = (year + "") + String.format("%02d", month) + String.format("%02d", day);
		sql = "select count(matchID) from matchinfo where matchID like'"
				+ matchId + "%'";
		insertedMatchIdCount = getmatchId(sql);
		matchId = matchId + (insertedMatchIdCount + 1);
		if (winBy == null)
			eliminatorResult = (String) ((HashMap) (matchInfo.get(CricketdataConstants.MATCHOUTCOME)))
					.get(CricketdataConstants.MATCHOUTCOMERESULT);

		if ("no result".equals(eliminatorResult)) {
			insertNoresultMatchInfo(matchInfo, insertDate, matchId, season);
			return matchId;
		}
		matchInfosql = getSqlInsertForMatch(matchInfo, matchId, insertDate,
				winBy, eliminatorResult, season);
		insertQueries.add(matchInfosql.toString());
		executeInsert(insertQueries);
		matchInfosql.delete(0, matchInfosql.length());
		return matchId;
	}


	
	/*
	 * 
	 * Function constructs Sql statements to insert into matchInfo table
	 */
	private static StringBuffer getSqlInsertForMatch(Map matchInfo,
			String matchId, StringBuffer insertDate, Map winBy,
			String eliminatorResult, String season) {
		log.info("Entering processFile");
		StringBuffer matchInfosql = new StringBuffer();
		String team1 = getTeamName(((ArrayList<String>) matchInfo.get(CricketdataConstants.TEAMS))
				.get(0));
		String team2 = getTeamName(((ArrayList<String>) matchInfo.get(CricketdataConstants.TEAMS))
				.get(1));
		String tossWinner = getTeamName((String) ((HashMap) (matchInfo
				.get("toss"))).get(CricketdataConstants.WINNER));
		String matchWinner = (String) ((HashMap) (matchInfo.get(CricketdataConstants.MATCHOUTCOME)))
				.get(CricketdataConstants.WINNER);
		if (matchWinner == null)
			matchWinner = CricketdataConstants.NR;
		else
			matchWinner = getTeamName(matchWinner);
		Map<String, Integer> playerIDlist = populatePlayerID();
		String manOfmatch = (String) ((ArrayList) (matchInfo.get(CricketdataConstants.MANOFMATCH))).get(0);
		int manOfMatchId = 0;
		if(playerIDlist.containsKey(manOfmatch)) {
			manOfMatchId = playerIDlist.get(manOfmatch);
		}
		matchInfosql
				.append("INSERT INTO matchInfo(MatchId,Venue,City,MatchDate,"
						+ "team1,team2,Winner,MOM,manOfMatchId,toss_winner,toss_winner_decision,season,won_by)"
						+ "VALUES (" + matchId + ",")
				.append("'")
				.append(matchInfo.get("venue"))
				.append("',")
				.append("'")
				.append(matchInfo.get("city"))
				.append("',DATE('")
				.append(insertDate)
				.append("'),")
				.append("'")
				.append(team1)
				.append("',")
				.append("'")
				.append(team2)
				.append("',")
				.append("'")
				.append(matchWinner)
				.append("',")
				.append("'")
				.append(((ArrayList) (matchInfo.get(CricketdataConstants.MANOFMATCH))).get(0))
				.append("',").append(manOfMatchId).append(",").append("'").append(tossWinner).append("',")
				.append("'")
				.append(((HashMap) (matchInfo.get("toss"))).get("decision"))
				.append("','").append(season).append("','");
		if (winBy != null) {
			if (winBy.containsKey(CricketdataConstants.WICKETS))
				matchInfosql.append(winBy.get(CricketdataConstants.WICKETS)).append(" wickets");
			else if (winBy.containsKey(CricketdataConstants.RUNS))
				matchInfosql.append(winBy.get(CricketdataConstants.RUNS)).append(" runs");
		} else {
			matchInfosql.append(eliminatorResult);
		}
		matchInfosql.append("')");
		return matchInfosql;
	}

	
	
	/*
	 * This function is used to insert records into matchInfo for those tables matches which dint yield a result
	 * 
	 */
	private static void insertNoresultMatchInfo(Map matchInfo, StringBuffer insertDate, String matchId, String season) {
		log.info("Entering insertNoresultMatchInfo");
		List<String> insertQueries = new ArrayList<String>();
		String team1 = getTeamName(((ArrayList<String>) matchInfo.get(CricketdataConstants.TEAMS))
				.get(0));
		String team2 = getTeamName(((ArrayList<String>) matchInfo.get(CricketdataConstants.TEAMS))
				.get(1));
		String tossWinner = getTeamName((String) ((HashMap) (matchInfo
				.get("toss"))).get(CricketdataConstants.WINNER));
		StringBuffer matchInfosql = new StringBuffer();
		matchInfosql
		.append("INSERT INTO matchInfo(MatchId,Venue,City,MatchDate,"
				+ "team1,team2,Winner,MOM,toss_winner,toss_winner_decision,season,won_by)"
				+ "VALUES (" + matchId + ",")
		.append("'")
		.append(matchInfo.get("venue"))
		.append("',")
		.append("'")
		.append(matchInfo.get("city"))
		.append("',DATE('")
		.append(insertDate)
		.append("'),")
		.append("'")
		.append(team1)
		.append("',")
		.append("'")
		.append(team2)
		.append("',")
		.append("'")
		.append("No Result")
		.append("',")
		.append("'")
		.append("")
		.append("',").append("'").append(tossWinner).append("',")
		.append("'")
		.append(((HashMap) (matchInfo.get("toss"))).get("decision"))
		.append("','").append(season).append("','").append("No Result").append("')");
		insertQueries.add(matchInfosql.toString());
		executeInsert(insertQueries);
	}

	
	
	/*This function is used to process player infoa nd isnert records into player_list_table table
	 * 
	 * 
	 */
	private static void processPlayerInfo(List<Set<String>> playersList) {
		log.info("Entering processPlayerInfo");
		Set<String> allPlayersList =  new HashSet<String>();
		Set<String> tempSet =  new HashSet<String>();
		if(playersList!=null && playersList.size()>0) {
			for (int i = 0; i < playersList.size(); i++) {
				tempSet = playersList.get(i);
				Iterator iter = tempSet.iterator();
				while (iter.hasNext()) {
					allPlayersList.add((String) iter.next());
				}
			}
		}
		insertElements(allPlayersList);		
	}

	/*
	 * function to construct queries to insert into playerList table
	 */
	private static void insertElements(Set<String> allPlayersList) {
		log.info("Entering insertElements");
		Iterator iter = allPlayersList.iterator();
		List<String> playerList = new ArrayList<String>();
		StringBuffer playerInsert =  new StringBuffer();
		while (iter.hasNext()) {
			playerInsert.append("INSERT IGNORE INTO player_list_table (PlayerName)values('")
			.append((String) iter.next()).append("')");
			playerList.add(playerInsert.toString());
			playerInsert.delete(0, playerInsert.length());
		}
		executeInsert(playerList);
	}

	
	/*
	 * Returns abbreviated team Name for the given full team name
	 */
	private static String getTeamName(String string) {
		log.info("Entering getTeamName");
		String teamName = "";
		switch (string) {
		case CricketdataConstants.CSK:
			teamName = "CSK";

			break;
		case CricketdataConstants.DC:
			teamName = "DC";

			break;
		case CricketdataConstants.DD:
			teamName = "DD";

			break;
		case CricketdataConstants.KKR:
			teamName = "KKR";

			break;
		case CricketdataConstants.KTK:
			teamName = "KTK";

			break;
		case CricketdataConstants.KX1P:
			teamName = "KX1P";

			break;
		case CricketdataConstants.MI:
			teamName = "MI";

			break;
		case CricketdataConstants.PWI:
			teamName = "PWI";

			break;
		case CricketdataConstants.PW:
			teamName = "PWI";

			break;
			
		case CricketdataConstants.RCB:
			teamName = "RCB";

			break;
		case CricketdataConstants.RR:
			teamName = "RR";

			break;
		case CricketdataConstants.SHR:
			teamName = "SHR";

			break;

		default:
			break;
		}
		return teamName;
	}

	/*
	 * Used to get list of playerId's from playerList table
	 */
	private static Map<String, Integer> populatePlayerID() {
		log.info("Entering populatePlayerID");
		String playerSql = "Select * from playerList";
		Connection conn = null;
		   Statement stmt = null;
		   Map<String,Integer> playerIDlist = new HashMap< String,Integer>();
		   try{
		      //STEP 3: Open a connection
			   conn = DataSource.getInstance().getConnection();
		      //STEP 4: Execute a query
		      stmt = conn.createStatement();
		      ResultSet rs = stmt.executeQuery(playerSql);
		      //STEP 5: Extract data from result set
		      while(rs.next()){
		         //Retrieve by column name
		    	  int playerID = rs.getInt(1);
		    	  String playerName = rs.getString(2);
		    	  playerIDlist.put(playerName,playerID );
		      }
		      //STEP 6: Clean-up environment
		      rs.close();
		      stmt.close();
		     conn.close();
		   }catch(SQLException se){
		      //Handle errors for JDBC
		      log.error(se);
		   }catch(Exception e){
		      //Handle errors for Class.forName
			   log.error(e);
		   }finally{
		      //finally block used to close resources
		      try{
		         if(stmt!=null)
		            stmt.close();
		      }catch(SQLException se2){
		    	  log.error(se2);
		      }// nothing we can do
		      if(conn!=null)
				try {
					conn.close();
				} catch (SQLException e) {
					log.error(e);
				}
		   }//end try
		  
		return playerIDlist;
	}
	
	/*
	 * function to get matchId while inserting the ballInfo records
	 */
	private static int getmatchId(String sql) {
		log.info("Entering getmatchId");
		Connection conn = null;
		   Statement stmt = null;
		   int countMatchID  = 0;
		   try{
		      //STEP 3: Open a connection
			   conn = DataSource.getInstance().getConnection();
		      //STEP 4: Execute a query
		      stmt = conn.createStatement();
		   
		      ResultSet rs = stmt.executeQuery(sql);

		      //STEP 5: Extract data from result set
		      while(rs.next()){
		         //Retrieve by column name
		    	  countMatchID  = rs.getInt(1);
		      }
		      //STEP 6: Clean-up environment
		      rs.close();
		      stmt.close();
		      conn.close();
		   }catch(SQLException se){
			   log.error(se);
		   }catch(Exception e){
		      //Handle errors for Class.forName
			   log.error(e);
		   }finally{
		      //finally block used to close resources
		      try{
		         if(stmt!=null)
		            stmt.close();
		      }catch(SQLException se2){
		    	  log.error(se2);
		      }// nothing we can do
		      if(conn!=null)
				try {
					conn.close();
				} catch (SQLException e) {
					 log.error(e);
				}
		   }//end try
		   return countMatchID;
	}

	/*
	 * Function to get IPL season No
	 */
	private static String getSeason(int year) {
		return String.format("%02d", ((year + 1) - CricketdataConstants.YEAR1));
	}

	public static void executeInsert(List<String> insertQueries) {
		log.info("Entering executeInsert");
		Connection conn = null;
		Statement stmt = null;
		try {
			
			// STEP 4: Execute a query
			log.debug("Creating statement...");
			
			for (int i = 0; i < insertQueries.size(); i++) {
				  conn = DataSource.getInstance().getConnection();
				stmt = conn.createStatement();
				stmt.executeUpdate(insertQueries.get(i));
				stmt.close();
				conn.close();
				//Thread.sleep(2);
			}
			//conn.close();
		} catch (SQLException se) {
			 log.error(se);
		} catch (Exception e) {
			// Handle errors for Class.forName
			 log.error(e);
		} finally {
			// finally block used to close resources
			try {
				if (stmt != null)
					stmt.close();
			} catch (SQLException se2) {
				 log.error(se2);
			}// nothing we can do
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					 log.error(e);
				}
		}// end try
		log.debug("Goodbye!");

	}

	/*
	 * function used to differentiate between innings and insert entries into ballInfo tables
	 */
private static Set<String> processinningsInfo(Map inningsInfo, String matchId) {
	log.info("Entering processinningsInfo");
	int inningsNo;
	List inningsDeliveryInfo = new ArrayList();
	String battingTeamName = null;
	Set<String> batsmanSet = null ;
	if(inningsInfo.containsKey(CricketdataConstants.FIRSTINNINGS)){
		inningsNo = 1;
		battingTeamName =   getTeamName((String)((HashMap)inningsInfo.get(CricketdataConstants.FIRSTINNINGS)).get(CricketdataConstants.TEAM));
		inningsDeliveryInfo = (ArrayList)((HashMap)inningsInfo.get(CricketdataConstants.FIRSTINNINGS)).get(CricketdataConstants.DELIVERIES);
		batsmanSet = storeDeliveryInfo(inningsDeliveryInfo,inningsNo,battingTeamName,matchId);
	}
	else if(inningsInfo.containsKey(CricketdataConstants.SECONDINNINGS)){
		inningsNo = 2;
		battingTeamName = getTeamName((String)((HashMap)inningsInfo.get(CricketdataConstants.SECONDINNINGS)).get(CricketdataConstants.TEAM));   
		inningsDeliveryInfo = (ArrayList)((HashMap)inningsInfo.get(CricketdataConstants.SECONDINNINGS)).get(CricketdataConstants.DELIVERIES);
		 batsmanSet = storeDeliveryInfo(inningsDeliveryInfo,inningsNo,battingTeamName,matchId);
	}
	return batsmanSet;
}

/*
 * Function to process information from yaml to be inserted into BallInfo table
 */
	private static Set<String> storeDeliveryInfo(List inningsDeliveryInfo, int inningsNo, String battingTeamName, String matchId) {
		log.info("Entering storeDeliveryInfo");
		Map inningsMap;
		List<String> insertQueries =  new ArrayList<String>();
		Set<String> batsmanSet = new HashSet<String>();
		StringBuffer sql = new StringBuffer();
		
		Map<String, Integer> playerIDlist = populatePlayerID();
		for (int i = 0; i < inningsDeliveryInfo.size(); i++) {
			inningsMap = (HashMap) inningsDeliveryInfo.get(i);
			Set es = inningsMap.entrySet();
			Iterator<Map.Entry> x = es.iterator();
			while (x.hasNext()) {
				Map.Entry y = x.next();
				Map runsMap = (HashMap) ((HashMap) y.getValue()).get(CricketdataConstants.RUNS);
				Map wicketsMap = (HashMap) ((HashMap) y.getValue())
						.get("wicket");
				Map extrasMap = (HashMap) ((HashMap) y.getValue()).get("extras");
				
				int batsManId = 0;
				int bowlerId = 0;
				int playeroutID = 0;
				String ballInfo = (String) (y.getKey()+"");
				ballInfo.indexOf(".");
				String overNo = ballInfo.substring(0, ballInfo.indexOf("."));
				String ballNo = ballInfo.substring((ballInfo.indexOf(".")+1), ballInfo.length());
				if(playerIDlist.containsKey((String)((HashMap) y.getValue()).get(CricketdataConstants.BATSMAN))) {
					batsManId = playerIDlist.get((String)((HashMap) y.getValue()).get(CricketdataConstants.BATSMAN));
				}
				if(playerIDlist.containsKey((String)((HashMap) y.getValue()).get(CricketdataConstants.BOWLER))) {
					bowlerId = playerIDlist.get((String)((HashMap) y.getValue()).get(CricketdataConstants.BOWLER));
				}
				sql.append("INSERT INTO BallInfo(MatchId,InningsNo,BattingTeam,overNo,BallNo,BatsManID,BowleriD,Batsman,Bowler,Nonstriker,RunsScoreByBat,Extras,"
						+ "TotalRunsScoreForBall,ExtrasType");
				if (wicketsMap != null && wicketsMap.size() > 0) {
					if (((ArrayList) (wicketsMap.get(CricketdataConstants.FIELDERS))) != null
							&& ((ArrayList) (wicketsMap.get(CricketdataConstants.FIELDERS)))
									.size() > 0)
						sql.append(",Fielders");
					sql.append(",Kind,PlayerOut,playerOutId,IS_OUT)");
				} else
					sql.append(")");
			
				batsmanSet.add((String) ((HashMap) y.getValue()).get(CricketdataConstants.BATSMAN));
				sql.append("VALUES (").append(matchId).append(",")
					.append("")
						 .append(inningsNo).append("")  
						.append(",'").append(battingTeamName).append("','")  
						.append(overNo)
						.append("'").append(",'")
						.append(ballNo)
						.append("'").append(",")
						.append(batsManId).append(",")
						.append(bowlerId).append(",'")
						.append(((HashMap) y.getValue()).get(CricketdataConstants.BATSMAN))
						.append("'").append(",'")
						.append(((HashMap) y.getValue()).get(CricketdataConstants.BOWLER))
						.append("'").append(",'")
						.append(((HashMap) y.getValue()).get(CricketdataConstants.NONSTRIKER))
						.append("'").append(",")
						.append(runsMap.get(CricketdataConstants.BATSMAN)).append("")
						.append(",").append(runsMap.get(CricketdataConstants.EXTRAS)).append("")
						.append(",").append(runsMap.get(CricketdataConstants.TOTAL)).append("");
				if(extrasMap!=null && extrasMap.size() >0) {
					if(extrasMap.containsKey("legbyes"))
						sql.append(",'").append("legbyes").append("'");
					else if(extrasMap.containsKey("wides"))
						sql.append(",'").append("wides").append("'");
					else if(extrasMap.containsKey("byes"))
						sql.append(",'").append("byes").append("'");
					else if(extrasMap.containsKey("noballs"))
						sql.append(",'").append("noballs").append("'");
					else if(extrasMap.containsKey("penalty"))
						sql.append(",'").append("byes").append("'");
				}
				else {
					sql.append(",'").append("").append("'");
				}
				
				if (wicketsMap != null && wicketsMap.size() > 0) {
					if (((ArrayList) (wicketsMap.get(CricketdataConstants.FIELDERS))) != null
							&& ((ArrayList) (wicketsMap.get(CricketdataConstants.FIELDERS)))
									.size() > 0)
						sql.append(",'")
								.append(((ArrayList) (wicketsMap
										.get(CricketdataConstants.FIELDERS))).get(0)).append("'");

					sql.append(",'").append(wicketsMap.get(CricketdataConstants.KIND)).append("'")
							.append(",'").append(wicketsMap.get(CricketdataConstants.PLAYEROUT));
					
					playeroutID = playerIDlist.get(wicketsMap.get(CricketdataConstants.PLAYEROUT));
					sql.append("'").append(",").append(playeroutID).append(",").append(new Boolean(true))
							.append(")");
				} else
					sql.append(")");
			}
			insertQueries.add(sql.toString());
			sql.delete(0, sql.length());
		}
		executeInsert(insertQueries);
		return batsmanSet;
	}



}