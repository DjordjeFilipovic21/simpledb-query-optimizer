package rs.raf.simpledb;
import java.util.Arrays;
import java.util.List;

import rs.raf.simpledb.parse.QueryData;
import rs.raf.simpledb.query.*;
import rs.raf.simpledb.query.aggregation.AggregationFn;
import rs.raf.simpledb.query.aggregation.CountFn;
import rs.raf.simpledb.query.aggregation.GroupByPlan;
import rs.raf.simpledb.query.operators.Scan;
import rs.raf.simpledb.record.Schema;
import rs.raf.simpledb.tx.Transaction;

import static rs.raf.simpledb.InitKolokvijumDB.*;


public class MainQueryRunner {


	public static void main(String[] args) {
		try {

			boolean isnew = initDB("kolokvijumdb");

			if (isnew){
				createDBTables();
				genericInsertDBData();
			}
			else
				System.out.println("StudentDB is already created!");

			/*
			Šema baze je sledeca:

			STUDENT(sid int, sname varchar(25), smerid int, goddipl int)
			SMER(smid int, smerName varchar(2  5))
			PREDMET(pid int, naziv varchar(25), smerid int)
			*/

			// Merenje brzine Cross Product Join
			long startTime1 = System.nanoTime();
			queryOptimized();
			long endTime1 = System.nanoTime();
			long duration1 = (endTime1 - startTime1) / 1_000_000; // konverzija u milisekunde

			System.out.println("\n===========================================");
			System.out.println("Cross Product Join time: " + duration1 + " ms");
			System.out.println("===========================================\n");

			// Merenje brzine Merge Sort Join
			long startTime2 = System.nanoTime();
			queryOptimizedMergeSortJoin();
			long endTime2 = System.nanoTime();
			long duration2 = (endTime2 - startTime2) / 1_000_000;

			System.out.println("\n===========================================");
			System.out.println("Merge Sort Join time: " + duration2 + " ms");
			System.out.println("===========================================\n");

			// Poređenje
			System.out.println("COMPARISON:");
			System.out.println("-----------");
			System.out.println("Cross Product Join: " + duration1 + " ms");
			System.out.println("Merge Sort Join:    " + duration2 + " ms");
			System.out.println("Difference:			" + Math.abs(duration1 - duration2) + " ms");

			if (duration1 < duration2) {
				double speedup = (double) duration2 / duration1;
				System.out.println("Cross Product is " + String.format("%.2f", speedup) + "x faster");
			} else {
				double speedup = (double) duration1 / duration2;
				System.out.println("Merge Sort is " + String.format("%.2f", speedup) + "x faster");
			}


		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	private static void queryManualPlan1() {

		/*
		 * Ovaj primer pokazuje manuelno kreiranje plana izvrsavanja upita.
		 * Zelimo da prikazemo sve studente koji su diplomirali 2025. godine.
		 * Ekvivalentan SQL upit bi bio:
		 * 	SELECT sname, goddipl
	       	FROM STUDENT
	        WHERE goddipl=2025

	        Obratiti paznju kako se kreiraju operatori Selekcije i Projekcije
	  	 *
		 */
		Transaction tx = new Transaction();
		Plan p1 = new TablePlan("student", tx);


		// Kreiranje predikatskog uslova 'goddipl=2025'
		Expression lhs1 = new FieldNameExpression("goddipl");
		Constant c = new IntConstant(2025);
		Expression rhs1 = new ConstantExpression(c);
		Term t1 = new Term(lhs1, rhs1);
		Predicate pred1 = new Predicate(t1); //goddipl=2025  (SI)

		Plan p3 = new SelectionPlan(p1, pred1);


		List<String> fields = Arrays.asList("sname", "goddipl");
		Plan p4 = new ProjectionPlan(p3, fields);

		Scan s = p4.open();

		System.out.println("\nStudenti sa smera Softversko inzenjerstvo:");
		System.out.println("\nStudent\t\t\tGodina diplomiranja");
		System.out.println("-----------------------------------------");
		while (s.next()) {
			String sname = s.getString("sname"); 	//SimpleDB cuva naziv kolona
			int god = s.getInt("goddipl"); 			//sa malim slovima (lower case)
			System.out.println(sname + "\t\t" + god);
		}
		System.out.println();
		s.close();
		tx.commit();

	}

	private static void queryManualPlan2() {

		/*
		 * Ovaj primer pokazuje manuelno kreiranje plana izvrsavanja upita.
		 * Zelimo da prikazemo sve studente sa smera Softversko inzenjerstvo.
		 * Ekvivalentan SQL upit bi bio:
		 * 	SELECT sname, smerName
	       	FROM STUDENT, SMER
	        WHERE smerId = SMId
	        AND	smerId = 1

	        Obratiti paznju da se equi-join izvodi kao:
	        SELEKCIJAuslov_spajanja(DEKARTOV_PROIZVOD(tabela1, tabela2))
	  	 *
		 */
		Transaction tx = new Transaction();
		Plan p1 = new TablePlan("student", tx);
		Plan p2 = new TablePlan("smer", tx);


		// Kreiranje predikatskog uslova 'smerid=1'
		Expression lhs1 = new FieldNameExpression("smerid");
		Constant c = new IntConstant(1);
		Expression rhs1 = new ConstantExpression(c);
		Term t1 = new Term(lhs1, rhs1);
		Predicate pred1 = new Predicate(t1); //smerid=1  (SI)

		Plan p3 = new SelectionPlan(p1, pred1);

		Plan p4 = new CrossProductPlan(p3, p2);

		// Kreiranje predikatskog uslova 'smerid=smid', sto je uslov spajanja/join-a
		Expression lhs2 = new FieldNameExpression("smerid");
		Expression rhs2 = new FieldNameExpression("smid");
		Term t2 = new Term(lhs2, rhs2);
		Predicate pred2 = new Predicate(t2); //smerid=smid  - Join uslov

		Plan p5 = new SelectionPlan(p4, pred2);


		List<String> fields = Arrays.asList("sname", "smername");
		Plan p6 = new ProjectionPlan(p5, fields);

		Scan s = p6.open();

		System.out.println("\nStudenti sa smera Softversko inzenjerstvo:");
		System.out.println("\nStudent\t\t\tSmer");
		System.out.println("-----------------------------------------");
		while (s.next()) {
			String sname = s.getString("sname"); 	//SimpleDB cuva naziv kolona
			String dname = s.getString("smername"); //sa malim slovima (lower case)
			System.out.println(sname + "\t\t" + dname);
		}
		System.out.println();
		s.close();
		tx.commit();

	}

	private static void queryOptimized() {
		Transaction tx = new Transaction();

		// 1. Filtriraj PREDMET (predGod = 1)
		Plan predmetPlan = new TablePlan("predmet", tx);
		Expression lhs1 = new FieldNameExpression("predgod");
		Expression rhs1 = new ConstantExpression(new IntConstant(1));
		Predicate predGod1 = new Predicate(new Term(lhs1, rhs1));
		Plan filteredPredmet = new SelectionPlan(predmetPlan, predGod1);

		// 2. Filtriraj POLAGANJE (ocena = 10)
		Plan polaganjePlan = new TablePlan("polaganje", tx);
		Expression lhs2 = new FieldNameExpression("ocena");
		Expression rhs2 = new ConstantExpression(new IntConstant(10));
		Predicate ocena10 = new Predicate(new Term(lhs2, rhs2));
		Plan filteredPolaganje = new SelectionPlan(polaganjePlan, ocena10);

		// 3. Join ISPIT i POLAGANJE (ispid = ispitid)
		Plan ispitPlan = new TablePlan("ispit", tx);
		Plan join1 = new CrossProductPlan(filteredPolaganje, ispitPlan);
		Expression lhs3 = new FieldNameExpression("ispid");
		Expression rhs3 = new FieldNameExpression("ispitid");
		Predicate joinPred1 = new Predicate(new Term(lhs3, rhs3));
		Plan join1Selected = new SelectionPlan(join1, joinPred1);

		// 4. Join sa PREDMET (pid = predmetid)
		Plan join2 = new CrossProductPlan(join1Selected, filteredPredmet);
		Expression lhs4 = new FieldNameExpression("pid");
		Expression rhs4 = new FieldNameExpression("predmetid");
		Predicate joinPred2 = new Predicate(new Term(lhs4, rhs4));
		Plan join2Selected = new SelectionPlan(join2, joinPred2);

		// 5. Join sa STUDENT (sid = polagStudId)
		Plan studentPlan = new TablePlan("student", tx);
		Plan join3 = new CrossProductPlan(join2Selected, studentPlan);
		Expression lhs5 = new FieldNameExpression("sid");
		Expression rhs5 = new FieldNameExpression("polagstudid");
		Predicate joinPred3 = new Predicate(new Term(lhs5, rhs5));
		Plan finalJoin = new SelectionPlan(join3, joinPred3);

		// 6. GROUP BY sid sa COUNT(ocena)
		List<String> groupFields = Arrays.asList("sid", "studname");
		List<AggregationFn> aggFns = Arrays.asList(
				new CountFn("ocena")
		);
		Plan groupPlan = new GroupByPlan(finalJoin, groupFields, aggFns, tx);

		// 7. ORDER BY desetke
		List<String> sortFields = Arrays.asList("countofocena");
		Plan sortPlan = new SortPlan(groupPlan, sortFields, tx);

		// 8. Projekcija finalnih kolona
		List<String> fields = Arrays.asList("studname", "countofocena");
		Plan finalPlan = new ProjectionPlan(sortPlan, fields);

		Scan s = finalPlan.open();
//
//		System.out.println("\nOptimizovani upit - Studenti sa desetkama:");
//		System.out.println("Student\t\t\tBroj desetki");
//		System.out.println("-----------------------------------------");
		int rownum = 0;
		while (s.next()) {
//			String studname = s.getString("studname");
//			int desetke = s.getInt("countofocena");
//			System.out.println(studname + "\t\t" + desetke + "\t\t" + ++rownum);
		}
		s.close();
		tx.commit();
	}

	private static void queryOptimizedMergeSortJoin() {
		Transaction tx = new Transaction();

		// 1. Filtriraj PREDMET (predGod = 1)
		Plan predmetPlan = new TablePlan("predmet", tx);
		Expression lhs1 = new FieldNameExpression("predgod");
		Expression rhs1 = new ConstantExpression(new IntConstant(1));
		Predicate predGod1 = new Predicate(new Term(lhs1, rhs1));
		Plan filteredPredmet = new SelectionPlan(predmetPlan, predGod1);

		// 2. Filtriraj POLAGANJE (ocena = 10)
		Plan polaganjePlan = new TablePlan("polaganje", tx);
		Expression lhs2 = new FieldNameExpression("ocena");
		Expression rhs2 = new ConstantExpression(new IntConstant(10));
		Predicate ocena10 = new Predicate(new Term(lhs2, rhs2));
		Plan filteredPolaganje = new SelectionPlan(polaganjePlan, ocena10);

		// 3. Join ISPIT i POLAGANJE (ispid = ispitid)
		Plan ispitPlan = new TablePlan("ispit", tx);
		Plan join1 = new MergeSortJoinPlan(filteredPolaganje, ispitPlan, "ispitid", "ispid", tx);
//		Expression lhs3 = new FieldNameExpression("ispid");
//		Expression rhs3 = new FieldNameExpression("ispitid");
//		Predicate joinPred1 = new Predicate(new Term(lhs3, rhs3));
//		Plan join1Selected = new SelectionPlan(join1, joinPred1);

		// 4. Merge join: Result ⋈ PREDMET (predmetid = pid)
		Plan join2 = new MergeSortJoinPlan(join1, filteredPredmet, "predmetid", "pid", tx);

		// 5. Merge join: Result ⋈ STUDENT (polagstudid = sid)
		Plan studentPlan = new TablePlan("student", tx);
		Plan join3 = new MergeSortJoinPlan(join2, studentPlan, "polagstudid", "sid", tx);

//		 6. GROUP BY sid sa COUNT(ocena)
		List<String> groupFields = Arrays.asList("sid", "studname");
		List<AggregationFn> aggFns = Arrays.asList(
				new CountFn("ocena")
		);
		Plan groupPlan = new GroupByPlan(join3, groupFields, aggFns, tx);

		// 7. ORDER BY desetke
		List<String> sortFields = Arrays.asList("countofocena");
		Plan sortPlan = new SortPlan(groupPlan, sortFields, tx);

		// 8. Projekcija finalnih kolona
		List<String> fields = Arrays.asList("studname");
		Plan finalPlan = new ProjectionPlan(sortPlan, fields);

		Scan s = finalPlan.open();
//
//		System.out.println("\nOptimizovani upit - Studenti sa desetkama:");
//		System.out.println("Student\t\t\tBroj desetki");
//		System.out.println("-----------------------------------------");
		int rownum = 0;
		while (s.next()) {
//			String studname = s.getString("studname");
//			System.out.println(studname  + "\t\t" + ++rownum);
		}
		s.close();
		tx.commit();
	}



//	private static void querySQL1() {
//		// Kreiranje transakcije
//		Transaction tx = new Transaction();
//
//
//
//		// Parsiranje upita i kreiranje plana izvrsavanja upita
//		//Plan p = SimpleDBEngine.planner().createQueryPlan(qry, tx);
//		QueryData queryResult =  SimpleDBEngine.planner().getParsingResult(sqlQuery);
//		System.out.println("Rezultat Parsiranja:\n"+queryResult);
//		Plan p = SimpleDBEngine.planner().createQueryPlanFromParsingResult(queryResult, tx);
//
//		System.out.println("\nLOGICKI PLAN UPITA:");
//		p.printPlan(0);
//
//		// Inicijalizacija plana upita, tj. svih operatora u stablu upita koji se izvrsavaju
//		Scan s = p.open();
//
//		System.out.println("\nSpisak svih studenata:\n");
//		System.out.println("Student\t\t\tSmer");
//		System.out.println("-----------------------------------------");
//		while (s.next()) {
//			String sname = s.getString("sname"); 	//SimpleDB cuva naziv kolona
//			String dname = s.getString("smername"); //sa malim slovima (lower case)
//			System.out.println(sname + "\t\t" + dname);
//		}
//		s.close();
//		tx.commit();
//	}


	/**
	    * Executes the specified SQL query string.
	    * The method calls the query planner to create a plan
	    * for the query.
	    */
	   public static void executeSQLQuery(String sqlQuery) {
		   Transaction tx = null;
		   try {
	         tx = new Transaction();
	         Plan plan = SimpleDBEngine.planner().createQueryPlan(sqlQuery, tx);
	         Scan scan = plan.open();
	         Schema sch = plan.schema();

	         while (scan.next()) {
	        	 //String sname = scan.getString("sname");
	        	 //int age = scan.getInt("age");


	         }

	      }
	      catch(RuntimeException e) {
	         tx.rollback();
	         throw e;
	      }
	   }

	   /**
	    * Executes the specified SQL update command.
	    * The method sends the command to the update planner,
	    * which executes it.
	   */
	   public static int executeSQLUpdate(String sqlCmd) {
		   Transaction tx = null;
		   try {
	         tx = new Transaction();
	         int result = SimpleDBEngine.planner().executeUpdate(sqlCmd, tx);

	         tx.commit();
	         return result;
	      }
	      catch(RuntimeException e) {
	         tx.rollback();
	         throw e;
	      }
	   }

//	   public static boolean initStudentDB() {
//			// Kreiranje baze podataka, sto podrazumeva fajl sa podacima i podatke u sistemskom katalogu
//			return SimpleDBEngine.init("studentdb");
//		}
//
//		public static void initStudentDBData() {
//			String createTableSQL = "create table STUDENT(sid int, sname varchar(25), smerId int, godDipl int)";
//
//			MainQueryRunner.executeSQLUpdate(createTableSQL);
//			System.out.println("Table STUDENT created.");
//
//			String insertSQLString = "insert into STUDENT(sid, sname, smerId, godDipl) values ";
//			String[] studvals = {"(1, 'Milan Petrovic', 1, 2025)",
//								 "(2, 'Jovan Jovanovic', 2, 2025)",
//								 "(3, 'Ana Mirkovic', 3, 2021)",
//								 "(4, 'Maja Spasic', 2, 2023)",
//								 "(5, 'Veljko Peric', 3, 2022)",
//								 "(6, 'Bojana Mijatovic', 1, 2025)",
//								 "(7, 'Lazar Kostic', 2, 2025)",
//								 "(8, 'Milica Milic', 3, 2024)",
//								 "(9, 'Nikola Urosevic', 2, 2022)"};
//			for (int i=0; i<studvals.length; i++)
//				MainQueryRunner.executeSQLUpdate(insertSQLString + studvals[i]);
//			System.out.println("STUDENT records inserted.");
//
//			createTableSQL = "create table SMER(smid int, smerName varchar(25))";
//			MainQueryRunner.executeSQLUpdate(createTableSQL);
//			System.out.println("Table SMER created.");
//
//			insertSQLString = "insert into SMER(smid, smerName) values ";
//			String[] deptvals = {"(1, 'Softversko inzenjerstvo')",
//								 "(2, 'Racunarsko inzenjerstvo')",
//								 "(3, 'Racunarske nauke')"};
//			for (int i=0; i<deptvals.length; i++)
//				MainQueryRunner.executeSQLUpdate(insertSQLString + deptvals[i]);
//			System.out.println("SMER records inserted.");
//
//			createTableSQL = "create table PREDMET(PId int, naziv varchar(25), smerId int)";
//			MainQueryRunner.executeSQLUpdate(createTableSQL);
//			System.out.println("Table PREDMET created.");
//
//			insertSQLString = "insert into PREDMET(PId, naziv, smerId) values ";
//			String[] coursevals = {"(12, 'Napredne Baze Podataka', 1)",
//								   "(22, 'Distribuirani Sistemi', 2)",
//								   "(32, 'Teorija Algoritama', 3)",
//								   "(42, 'Diskretna Matematika', 3)",
//								   "(52, 'Racunarske Mreze', 2)",
//								   "(62, 'Softverske metodologije', 1)"};
//			for (int i=0; i<coursevals.length; i++)
//				MainQueryRunner.executeSQLUpdate(insertSQLString + coursevals[i]);
//			System.out.println("PREDMET records inserted.");
//
//			/*
//			s = "create table SECTION(SectId int, CourseId int, Prof varchar(8), YearOffered int)";
//			stmt.executeUpdate(s);
//			System.out.println("Table SECTION created.");
//
//			s = "insert into SECTION(SectId, CourseId, Prof, YearOffered) values ";
//			String[] sectvals = {"(13, 12, 'turing', 2004)",
//								 "(23, 12, 'turing', 2005)",
//								 "(33, 32, 'newton', 2000)",
//								 "(43, 32, 'einstein', 2001)",
//								 "(53, 62, 'brando', 2001)"};
//			for (int i=0; i<sectvals.length; i++)
//				stmt.executeUpdate(s + sectvals[i]);
//			System.out.println("SECTION records inserted.");
//
//			s = "create table ENROLL(EId int, StudentId int, SectionId int, Grade varchar(2))";
//			stmt.executeUpdate(s);
//			System.out.println("Table ENROLL created.");
//
//			s = "insert into ENROLL(EId, StudentId, SectionId, Grade) values ";
//			String[] enrollvals = {"(14, 1, 13, 'A')",
//								   "(24, 1, 43, 'C' )",
//								   "(34, 2, 43, 'B+')",
//								   "(44, 4, 33, 'B' )",
//								   "(54, 4, 53, 'A' )",
//								   "(64, 6, 53, 'A' )"};
//			for (int i=0; i<enrollvals.length; i++)
//				stmt.executeUpdate(s + enrollvals[i]);
//			System.out.println("ENROLL records inserted.");
//			*/
//		}

}
