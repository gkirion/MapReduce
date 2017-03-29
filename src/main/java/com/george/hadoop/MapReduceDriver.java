package com.george.hadoop;

import org.apache.hadoop.util.ProgramDriver;

public class MapReduceDriver {
	
	public static void main(String args[]) {
	    int exitCode = -1;
	    ProgramDriver pgd = new ProgramDriver();
	    try {
	    	pgd.addClass("wikipediasort", WikipediaSort.class, "wikipedia sort");
	    	pgd.addClass("wikipediahistogramapprox", WikipediaHistogramApprox.class, "wikipedia histogram approx");
	    	pgd.addClass("wikipediahistogramfull", WikipediaHistogramFull.class, "wikipedia histogram full");
	    	pgd.addClass("popularkeywords", PopularKeywords.class, "popular keywords");
	    	pgd.addClass("popularpages", PopularPages.class, "popular pages");
	    	pgd.addClass("searchhits", SearchHits.class, "search hits");
	    	pgd.addClass("perdaysearches", PerDaySearches.class, "per day searches");
	    	pgd.addClass("perweeksearches", PerWeekSearches.class, "per week searches");
	    	pgd.addClass("searchanalytics", SearchAnalytics.class, "search analytics");
	    	pgd.addClass("finddupes", FindDupes.class, "find duplicates on a text");
	    	pgd.addClass("lettercount", LetterCount.class, "a rouxlas production.");
	    	pgd.addClass("twitter", Twitter.class, "a twitter message counter.");
	    	exitCode = pgd.run(args);
	    }
	    catch(Throwable e){
	    	e.printStackTrace();
	    }
	    System.exit(exitCode);
	}
	
}
