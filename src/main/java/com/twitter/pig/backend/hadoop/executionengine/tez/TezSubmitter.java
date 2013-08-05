package com.twitter.pig.backend.hadoop.executionengine.tez;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.committer.VertexContext;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TezSubmitter {

	private TezConfiguration conf;
	private TezClient tezClient;
	private long pollTime; 
	private TezLauncher launcher;
	private Map<TezJob, List<TezJob>> jobDependencies = Collections.synchronizedMap(new HashMap<TezJob, List<TezJob>>());
	private List<TezJob> jobs = new LinkedList<TezJob>();
	private Set<TezJob> succeededJobs = Collections.synchronizedSet(new HashSet<TezJob>());
	private int numThreads;
	private ExecutorService executor;
	
	public TezSubmitter(TezLauncher launcher, TezConfiguration conf, int numThreads) {
		this.launcher = launcher; 
		this.tezClient = launcher.getTezClient();
		this.pollTime = conf.getLong("tez.dagclient.poll-interval", 60000);
		this.numThreads = numThreads;
	}

	public TezSubmitter(TezLauncher launcher, Map<TezJob, List<TezJob>> jobs, TezConfiguration conf, int numThreads) {
		this.launcher = launcher; 
		this.tezClient = launcher.getTezClient();
		this.pollTime = conf.getLong("tez.dagclient.poll-interval", 60000);
		this.numThreads = numThreads;
		this.jobDependencies.putAll(jobs);
		this.jobs.addAll(this.jobDependencies.keySet());
	}
	

	public void submitJob(TezJob job, List<TezJob> dependencies) {
		


	}


	public void start() {

		//LOG.debug("Starting thread pool executor.");
		//LOG.debug("Max local threads: " + maxMapThreads);
		//LOG.debug("Map tasks to process: " + numMapTasks);

		// Create a new executor service to drain the work queue.
		ThreadFactory tf = new ThreadFactoryBuilder()
		.setNameFormat("TezSubmitter Submit&Monitor Task #%d")
		.build();
		this.executor = Executors.newFixedThreadPool(numThreads, tf);
		List<DAGProgressNotificationListener> listeners = new ArrayList<DAGProgressNotificationListener>();
		List<TezJob> readyJobs = getReadyJobs();
		for (TezJob job : readyJobs) {
			job.submitJob(tezClient);
			executor.submit(job.getMonitorRunnable(pollTime, listeners));
		}
	}
	
	private List<TezJob> getReadyJobs() {
		synchronized(jobs) {
			/*
			for (TezJob job : jobs) {
				job.
			}
			*/
		}
		return jobs;
	}
	
	
	public void stop() throws Exception {

		try {
			executor.shutdown(); // Instructs queue to drain.

			// Wait for tasks to finish; do not use a time-based timeout.
			// (See http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=6179024)
			//LOG.info("Waiting for map tasks");
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		} catch (InterruptedException ie) {
			// Cancel all threads.
			executor.shutdownNow();
			throw ie;
		}

		//LOG.info("Map task executor complete.");

		// After waiting for the map tasks to complete, if any of these
		// have thrown an exception, rethrow it now in the main thread context.
		/*
		for (TezJob.Monitor r : taskRunnables) {
			if (r.storedException != null) {
				throw new Exception(r.storedException);
			}
		}
		*/

	}



}

/*

	public void monitor() {

		while(true) {

			try {Thread.sleep(pollTime);} 
			catch (InterruptedException e) {}

			try {
				DAGStatus dagStatus = dagClient.getDAGStatus();
				State dagState = dagStatus.getState();
				switch (dagState) {			
				case SUCCEEDED: 
					//launcher.jobCompleted(job);
					return;
				case FAILED:
				case KILLED:
					//launcher.jobFailed(job);
					return;
				default: 
					//do nothing; dag is still running
				}
				//Map<String, Progress> vertexProgress = dagStatus.getVertexProgress();
			} catch (IOException | TezException e) {}
		}

	}

 */

/*
	public void emitJobsSubmittedNotification() {

	}

	public void emitJobFailedNotification(JobStats jobStats) {
        for (PigProgressNotificationListener listener: listeners) {
            listener.jobFailedNotification(id, jobStats);
        }
    }


	public DAG getDAG() {
		return dag; 
	}

	public void setDAG(DAG dag) {
		if (dagStatus == null) {
			this.dag = dag; 
		} else {
			throw Exception();
		}
	}

 */


