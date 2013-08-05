package com.twitter.pig.backend.hadoop.executionengine.tez;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.Job.JobState;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;

import com.twitter.pig.backend.hadoop.executionengine.tez.DAGProgressNotificationListener;

public class TezJob extends Job {

	private TezConfiguration configuration;
	private ApplicationId appId;
	private DAG dag;
	private Path appStagingDir;
	private Credentials ts;
	private String amQueueName;
	private List<String> amArgs;
	private Map<String, String> amEnv;
	private Map<String, LocalResource> amLocalResources;
	private DAGClient dagClient;
	private DAGStatus dagStatus;

	public TezJob(TezConfiguration conf) throws IOException {
		super(new JobConf(conf));
		this.configuration = conf;
		// TODO Auto-generated constructor stub
	}


	public TezJob(TezConfiguration conf,ApplicationId appId, DAG dag,
			Path appStagingDir, Credentials ts, String amQueueName,
			List<String> amArgs, Map<String, String> amEnv,
			Map<String, LocalResource> amLocalResources) throws IOException {
		super(new JobConf(conf));
		this.appId = appId; 
		this.dag = dag;
		this.appStagingDir = appStagingDir; 
		this.ts = ts;
		this.amQueueName = amQueueName;
		this.amArgs = amArgs;
		this.amEnv = amEnv;
		this.amLocalResources = amLocalResources;
		this.configuration = conf;
	}


	public void submitJob(TezClient tezClient) {

		try {
			this.dagClient = tezClient.submitDAGApplication(appId, dag, appStagingDir,
					ts, amQueueName, amArgs, amEnv, amLocalResources, configuration);
			updateDAGStatus();
		} catch (IOException | TezException e) {
			e.printStackTrace();
		}
	}

	public VertexStatus getVertexStatus(String vertexName) {

		VertexStatus vertexStatus = null;
		try {
			vertexStatus = dagClient.getVertexStatus(vertexName);
		} catch (IOException | TezException e) {
			e.printStackTrace();
		}
		return vertexStatus;
	}

	public DAGStatus getDAGStatus() {
		updateDAGStatus();
		return this.dagStatus;
	}

	public DAGStatus.State getDAGState() {
		updateDAGStatus();
		return this.dagStatus.getState();
	}

	public void updateDAGStatus() {
		try {
			this.dagStatus = dagClient.getDAGStatus();
		} catch (IOException | TezException e) {
			e.printStackTrace();
		}
	}



	/**
	 * @return the configuration
	 */
	public TezConfiguration getConfiguration() {
		return configuration;
	}


	/**
	 * @param configuration the configuration to set
	 */
	public void setConfiguration(TezConfiguration configuration) {
		this.configuration = configuration;
	}


	/**
	 * @return the appId
	 */
	public ApplicationId getAppId() {
		return appId;
	}


	/**
	 * @param appId the appId to set
	 */
	public void setAppId(ApplicationId appId) {
		this.appId = appId;
	}


	/**
	 * @return the dag
	 */
	public DAG getDag() {
		return dag;
	}


	/**
	 * @param dag the dag to set
	 */
	public void setDag(DAG dag) {
		this.dag = dag;
	}


	/**
	 * @return the appStagingDir
	 */
	public Path getAppStagingDir() {
		return appStagingDir;
	}


	/**
	 * @param appStagingDir the appStagingDir to set
	 */
	public void setAppStagingDir(Path appStagingDir) {
		this.appStagingDir = appStagingDir;
	}


	/**
	 * @return the ts
	 */
	public Credentials getTs() {
		return ts;
	}


	/**
	 * @param ts the ts to set
	 */
	public void setTs(Credentials ts) {
		this.ts = ts;
	}


	/**
	 * @return the amQueueName
	 */
	public String getAmQueueName() {
		return amQueueName;
	}


	/**
	 * @param amQueueName the amQueueName to set
	 */
	public void setAmQueueName(String amQueueName) {
		this.amQueueName = amQueueName;
	}


	/**
	 * @return the amArgs
	 */
	public List<String> getAmArgs() {
		return amArgs;
	}


	/**
	 * @param amArgs the amArgs to set
	 */
	public void setAmArgs(List<String> amArgs) {
		this.amArgs = amArgs;
	}


	/**
	 * @return the amEnv
	 */
	public Map<String, String> getAmEnv() {
		return amEnv;
	}


	/**
	 * @param amEnv the amEnv to set
	 */
	public void setAmEnv(Map<String, String> amEnv) {
		this.amEnv = amEnv;
	}


	/**
	 * @return the amLocalResources
	 */
	public Map<String, LocalResource> getAmLocalResources() {
		return amLocalResources;
	}


	/**
	 * @param amLocalResources the amLocalResources to set
	 */
	public void setAmLocalResources(Map<String, LocalResource> amLocalResources) {
		this.amLocalResources = amLocalResources;
	}


	public boolean isComplete() throws IOException {
		//ensureState(JobState.RUNNING);
		updateDAGStatus();
		return dagStatus.isCompleted();
	}

	/**
	 * Check if the job completed successfully. 
	 * 
	 * @return <code>true</code> if the job succeeded, else <code>false</code>.
	 * @throws IOException
	 */
	public boolean isSuccessful() throws IOException {
		//ensureState(JobState.RUNNING);
		updateDAGStatus();
		return dagStatus.getState() == DAGStatus.State.SUCCEEDED;
	}
	


	class Monitor implements Runnable {

		private long pollTime;
		private List<DAGProgressNotificationListener> listeners;

		Monitor(long pollTime, List<DAGProgressNotificationListener> listeners) {
			this.pollTime = pollTime;
			this.listeners = listeners;
		}


		private void emitDagStateUpdate(DAGStatus.State state) {
			for (DAGProgressNotificationListener listener: listeners) {
				listener.dagStateUpdate(state);
			}
		}



		public void run() {

			String lastReport = null;
			Configuration clientConf = getConfiguration();
			//JobID jobId = getJobID();
			//LOG.info("Running job: " + jobId);
			//int progMonitorPollIntervalMillis = 
			//		Job.getProgressPollInterval(clientConf);
			/* make sure to report full progress after the job is done */
			boolean reportedAfterCompletion = false;
			//boolean reportedUberMode = false;
			DAGStatus.State lastState = dagStatus.getState();
			emitDagStateUpdate(lastState);
			boolean isComplete = false;
			while (!isComplete || !reportedAfterCompletion) {

				try {
					isComplete = isComplete();
					
					if (isComplete()) {
						reportedAfterCompletion = true;
					} else {
						Thread.sleep(pollTime);
					}
				} catch (IOException | InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	

				if (dagStatus.getState() != lastState) {
					emitDagStateUpdate(dagStatus.getState());
				}
				if (dagStatus.getState() == DAGStatus.State.SUBMITTED || dagStatus.getState() == DAGStatus.State.INITING) {
					continue;
				}      
				/*
				if (!reportedUberMode) {
					reportedUberMode = true;
					LOG.info("Job " + jobId + " running in uber mode : " + isUber());
				} 
				 */
				/*
				String report = 
						(" map " + StringUtils.formatPercent(mapProgress(), 0)+
								" reduce " + 
								StringUtils.formatPercent(reduceProgress(), 0));
				if (!report.equals(lastReport)) {
					LOG.info(report);
					lastReport = report;
				}

				TaskCompletionEvent[] events = 
						getTaskCompletionEvents(eventCounter, 10); 
				eventCounter += events.length;		
				 */
			}
			try {
				boolean success = isSuccessful();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			/*
			if (success) {
				LOG.info("Job " + jobId + " completed successfully");
			} else {
				LOG.info("Job " + jobId + " failed with state " + status.getState() + 
						" due to: " + status.getFailureInfo());
			}

			Counters counters = getCounters();
			if (counters != null) {
				LOG.info(counters.toString());
			}
			 */
			//return success;
		}



		/*
		private void printTaskEvents(TaskCompletionEvent[] events,
				Job.TaskStatusFilter filter, boolean profiling, IntegerRanges mapRanges,
				IntegerRanges reduceRanges) throws IOException, InterruptedException {
			for (TaskCompletionEvent event : events) {
				switch (filter) {
				case NONE:
					break;
				case SUCCEEDED:
					if (event.getStatus() == 
					TaskCompletionEvent.Status.SUCCEEDED) {
						LOG.info(event.toString());
					}
					break; 
				case FAILED:
					if (event.getStatus() == 
					TaskCompletionEvent.Status.FAILED) {
						LOG.info(event.toString());
						// Displaying the task diagnostic information
						TaskAttemptID taskId = event.getTaskAttemptId();
						String[] taskDiagnostics = getTaskDiagnostics(taskId); 
						if (taskDiagnostics != null) {
							for (String diagnostics : taskDiagnostics) {
								System.err.println(diagnostics);
							}
						}
					}
					break; 
				case KILLED:
					if (event.getStatus() == TaskCompletionEvent.Status.KILLED){
						LOG.info(event.toString());
					}
					break; 
				case ALL:
					LOG.info(event.toString());
					break;
				}
			}
		}

	}
		 */

	}
	
	public Monitor getMonitorRunnable(long pollTime, List<DAGProgressNotificationListener> listeners) {
		return new Monitor(pollTime, listeners);
		
	}

}
