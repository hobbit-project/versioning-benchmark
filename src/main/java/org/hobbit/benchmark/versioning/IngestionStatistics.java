/**
 * 
 */
package org.hobbit.benchmark.versioning;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.hobbit.benchmark.versioning.components.VersioningEvaluationModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author papv
 *
 */
public class IngestionStatistics {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(VersioningEvaluationModule.class);

	// the total number of triples up to every version
	private HashMap<Integer,Integer> totalTriples = new HashMap<Integer,Integer>();
	// the total number of each version's changes with respect to the previous one
	private HashMap<Integer,Long> loadingTimes = new HashMap<Integer,Long>();
	
	private AtomicLong runsCount;
	private AtomicLong failuresCount;
	
	private boolean changesComputed = false;
	private float avgChangesPS = 0;
		
	public synchronized void reportSuccess(int version, int loadedTriples, long currentLoadingTimeMs) {
		loadingTimes.put(version, currentLoadingTimeMs);
		totalTriples.put(version, loadedTriples);
	}
	
	public void reportFailure() {
		failuresCount.incrementAndGet();
	}
	
	public float getInitialVersionIngestionSpeed() {
		long initVersionLoadingTimeMS = loadingTimes.get(0);
		int initVersionTriples = totalTriples.get(0);
		// result should be returned in seconds
		return  initVersionTriples / (initVersionLoadingTimeMS / 1000f);
	}
	
	private void computeChanges() {
		// TODO: have to be changed in the 2nd version of the benchmark in which
		// there will be deletions, or changes too
		for(int i=1; i<totalTriples.size(); i++) {
			int changedTriples = getVersionTriples(i) - getVersionTriples(i-1);
			float loadingTime = loadingTimes.get(i) / 1000f;
			avgChangesPS += changedTriples / loadingTime;
		}
		changesComputed = true;
	}
	
	public double getAvgChangesPS() {
		if(!changesComputed) {
			computeChanges();
		}
		return avgChangesPS / (loadingTimes.size() - 1);
	}
	
	public int getVersionTriples(int version) {
		return totalTriples.get(version);
	}
	
	public long getRunsCount() {
		return runsCount.get();
	}
	
	public long getFailuresCount() {
		return failuresCount.get();
	}	
}
