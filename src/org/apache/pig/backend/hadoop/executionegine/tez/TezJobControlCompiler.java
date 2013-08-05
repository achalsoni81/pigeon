/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.executionegine.tez;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.pig.ComparisonFunc;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigException;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigFloatWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigGroupingFloatWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.*;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigLongWritableComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigBigDecimalRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigBigIntegerRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigBooleanRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigBytesRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigDateTimeRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigDoubleRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigFloatRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigIntRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigLongRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapOnly;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextRawComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTupleSortComparator;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.SecondaryKeyPartitioner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.SkewedPartitioner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.WeightedRangePartitioner;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeCogroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataType;
import org.apache.pig.data.SchemaTupleFrontend;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.NullableBigDecimalWritable;
import org.apache.pig.impl.io.NullableBigIntegerWritable;
import org.apache.pig.impl.io.NullableBooleanWritable;
import org.apache.pig.impl.io.NullableBytesWritable;
import org.apache.pig.impl.io.NullableDateTimeWritable;
import org.apache.pig.impl.io.NullableDoubleWritable;
import org.apache.pig.impl.io.NullableFloatWritable;
import org.apache.pig.impl.io.NullableIntWritable;
import org.apache.pig.impl.io.NullableLongWritable;
import org.apache.pig.impl.io.NullablePartitionWritable;
import org.apache.pig.impl.io.NullableText;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.Pair;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.EdgeProperty.ConnectionPattern;
import org.apache.tez.dag.api.EdgeProperty.SourceType;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.engine.lib.input.ShuffledMergedInput;
import org.apache.tez.engine.lib.output.OnFileSortedOutput;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.hadoop.MRJobConfig;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfToTezTranslator;
import org.apache.tez.mapreduce.hadoop.MultiStageMRConfigUtil;
import org.apache.tez.mapreduce.processor.map.MapProcessor;
import org.apache.tez.mapreduce.processor.reduce.ReduceProcessor;
import org.apache.pig.backend.hadoop.executionengine.shims.HadoopShims;

import com.google.common.annotations.VisibleForTesting;

/**
 * This is compiler class that takes an MROperPlan and converts
 * it into a JobControl object with the relevant dependency info
 * maintained. The JobControl Object is made up of Jobs each of
 * which has a JobConf. The MapReduceOper corresponds to a Job
 * and the getJobCong method returns the JobConf that is configured
 * as per the MapReduceOper
 *
 * <h2>Comparator Design</h2>
 * <p>
 * A few words on how comparators are chosen.  In almost all cases we use raw
 * comparators (the one exception being when the user provides a comparison
 * function for order by).  For order by queries the PigTYPERawComparator
 * functions are used, where TYPE is Int, Long, etc.  These comparators are
 * null aware and asc/desc aware.  The first byte of each of the
 * NullableTYPEWritable classes contains info on whether the value is null.
 * Asc/desc is written as an array into the JobConf with the key pig.sortOrder
 * so that it can be read by each of the comparators as part of their
 * setConf call.
 * <p>
 * For non-order by queries, PigTYPEWritableComparator classes are used.
 * These are all just type specific instances of WritableComparator.
 *
 */
public class TezJobControlCompiler{
	TezOperPlan plan;
	TezConfiguration conf;
	PigContext pigContext;

	private static final Log log = LogFactory.getLog(TezJobControlCompiler.class);

	public static final String LOG_DIR = "_logs";

	public static final String END_OF_INP_IN_MAP = "pig.invoke.close.in.map";

	private static final String REDUCER_ESTIMATOR_KEY = "pig.exec.reducer.estimator";
	private static final String REDUCER_ESTIMATOR_ARG_KEY =  "pig.exec.reducer.estimator.arg";

	public static final String PIG_MAP_COUNTER = "pig.counters.counter_";
	public static final String PIG_MAP_RANK_NAME = "pig.rank_";
	public static final String PIG_MAP_SEPARATOR = "_";
	public HashMap<String, ArrayList<Pair<String,Long>>> globalCounters = new HashMap<String, ArrayList<Pair<String,Long>>>();

	/**
	 * We will serialize the POStore(s) present in map and reduce in lists in
	 * the Hadoop Conf. In the case of Multi stores, we could deduce these from
	 * the map plan and reduce plan but in the case of single store, we remove
	 * the POStore from the plan - in either case, we serialize the POStore(s)
	 * so that PigOutputFormat and PigOutputCommiter can get the POStore(s) in
	 * the same way irrespective of whether it is multi store or single store.
	 */
	public static final String PIG_MAP_STORES = "pig.map.stores";
	public static final String PIG_REDUCE_STORES = "pig.reduce.stores";

	// A mapping of job to pair of store locations and tmp locations for that job
	private Map<Job, Pair<List<POStore>, Path>> jobStoreMap;

	private Map<TezJob, ArrayList<TezOperator>> jobTezOpersMap;
	private int counterSize;
	private TezClient tezClient;

	public TezJobControlCompiler(PigContext pigContext, Configuration conf, TezClient tezClient) throws IOException {
		this.pigContext = pigContext;
		this.conf = new TezConfiguration(conf);
		this.tezClient = tezClient;
		jobStoreMap = new HashMap<Job, Pair<List<POStore>, Path>>();
		jobTezOpersMap = new HashMap<TezJob, ArrayList<TezOperator>>();
	}

	/**
	 * Returns all store locations of a previously compiled job
	 */
	public List<POStore> getStores(Job job) {
		Pair<List<POStore>, Path> pair = jobStoreMap.get(job);
		if (pair != null && pair.first != null) {
			return pair.first;
		} else {
			return new ArrayList<POStore>();
		}
	}

	/**
	 * Resets the state
	 */
	public void reset() {
		jobStoreMap = new HashMap<Job, Pair<List<POStore>, Path>>();
		jobTezOpersMap = new HashMap<TezJob, ArrayList<TezOperator>>();
		UDFContext.getUDFContext().reset();
	}

	/**
	 * Gets the map of Job and the MR Operator
	 */
	public Map<TezJob, ArrayList<TezOperator>> getJobTezOpersMap() {
		return Collections.unmodifiableMap(jobTezOpersMap);
	}

	/**
	 * Moves all the results of a collection of MR jobs to the final
	 * output directory. Some of the results may have been put into a
	 * temp location to work around restrictions with multiple output
	 * from a single map reduce job.
	 *
	 * This method should always be called after the job execution
	 * completes.
	 */
	public void moveResults(List<Job> completedJobs) throws IOException {
		for (Job job: completedJobs) {
			Pair<List<POStore>, Path> pair = jobStoreMap.get(job);
			if (pair != null && pair.second != null) {
				Path tmp = pair.second;
				Path abs = new Path(tmp, "abs");
				Path rel = new Path(tmp, "rel");
				FileSystem fs = tmp.getFileSystem(conf);

				if (fs.exists(abs)) {
					moveResults(abs, abs.toUri().getPath(), fs);
				}

				if (fs.exists(rel)) {
					moveResults(rel, rel.toUri().getPath()+"/", fs);
				}
			}
		}
	}

	/**
	 * Walks the temporary directory structure to move (rename) files
	 * to their final location.
	 */
	private void moveResults(Path p, String rem, FileSystem fs) throws IOException {
		for (FileStatus fstat: fs.listStatus(p)) {
			Path src = fstat.getPath();
			if (fstat.isDir()) {
				log.info("mkdir: "+src);
				fs.mkdirs(removePart(src, rem));
				moveResults(fstat.getPath(), rem, fs);
			} else {
				Path dst = removePart(src, rem);
				log.info("mv: "+src+" "+dst);
				fs.rename(src,dst);
			}
		}
	}

	private Path removePart(Path src, String part) {
		URI uri = src.toUri();
		String pathStr = uri.getPath().replace(part, "");
		return new Path(pathStr);
	}

	/**
	 * Compiles all jobs that have no dependencies removes them from
	 * the plan and returns. Should be called with the same plan until
	 * exhausted.
	 * @param plan - The MROperPlan to be compiled
	 * @param grpName - The name given to the JobControl
	 * @return JobControl object - null if no more jobs in plan
	 * @throws JobCreationException
	 */
	public ArrayList<TezJob> compile(TezOperPlan plan, String grpName) throws JobCreationException{
		// Assert plan.size() != 0
		this.plan = plan;

		int timeToSleep;
		String defaultPigJobControlSleep = pigContext.getExecType() == ExecType.TEZ_LOCAL ? "100" : "5000";
		String pigJobControlSleep = conf.get("pig.jobcontrol.sleep", defaultPigJobControlSleep);
		if (!pigJobControlSleep.equals(defaultPigJobControlSleep)) {
			log.info("overriding default JobControl sleep (" + defaultPigJobControlSleep + ") to " + pigJobControlSleep);
		}

		try {
			timeToSleep = Integer.parseInt(pigJobControlSleep);
		} catch (NumberFormatException e) {
			throw new RuntimeException("Invalid configuration " +
					"pig.jobcontrol.sleep=" + pigJobControlSleep +
					" should be a time in ms. default=" + defaultPigJobControlSleep, e);
		}

		ArrayList<TezJob> tezJobs = new ArrayList<TezJob>();

		try {
			List<TezOperator> roots = new LinkedList<TezOperator>();
			roots.addAll(plan.getRoots());
			for (TezOperator root: roots) {

				TezOperPlan subDAG = getMRRChain(plan, root);
				TezJob job = getJob(subDAG, conf, pigContext);
				ArrayList<TezOperator> tezOpers = new ArrayList<TezOperator>();
				for (TezOperator tezOper : subDAG.getKeys().values()) {
					tezOpers.add(tezOper);
				}
				jobTezOpersMap.put(job, tezOpers);
				tezJobs.add(job);
			}
		} catch (JobCreationException jce) {
			throw jce;
		} catch(Exception e) {
			int errCode = 2017;
			String msg = "Internal error creating job configuration.";
			throw new JobCreationException(msg, errCode, PigException.BUG, e);
		}

		return tezJobs;
	}

	// Update Map-Reduce plan with the execution status of the jobs. If one job
	// completely fail (the job has only one store and that job fail), then we
	// remove all its dependent jobs. This method will return the number of MapReduceOper
	// removed from the Map-Reduce plan
	public int updateTezOperPlan(List<TezJob> completeFailedJobs)
	{
		int sizeBefore = plan.size();
		/*
		for (Job job : completeFailedJobs)  // remove all subsequent jobs
		{
			MapReduceOper mrOper = jobTezOpersPlan.get(job);
			plan.trimBelow(mrOper);
			plan.remove(mrOper);
		}
		*/

		// Remove successful jobs from jobMroMap
		for (Job job : jobTezOpersMap.keySet())
		{
			if (!completeFailedJobs.contains(job))
			{
				ArrayList<TezOperator> tezOpers = jobTezOpersMap.get(job);
				/*
				if (!pigContext.inIllustrator && mro.isCounterOperation())
					saveCounters(job,mro.getOperationID());
				*/
				
				for (TezOperator tezOper : tezOpers) {
					plan.remove(tezOper);
				}
			}
		}


		jobTezOpersMap.clear();
		int sizeAfter = plan.size();
		return sizeBefore-sizeAfter;
	}

	/**
	 * Reads the global counters produced by a job on the group labeled with PIG_MAP_RANK_NAME.
	 * Then, it is calculated the cumulative sum, which consists on the sum of previous cumulative
	 * sum plus the previous global counter value.
	 * @param job with the global counters collected.
	 * @param operationID After being collected on global counters (POCounter),
	 * these values are passed via configuration file to PORank, by using the unique
	 * operation identifier
	 */
	private void saveCounters(Job job, String operationID) {
		Counters counters;
		Group groupCounters;

		Long previousValue = 0L;
		Long previousSum = 0L;
		ArrayList<Pair<String,Long>> counterPairs;

		try {
			counters = HadoopShims.getCounters(job);
			groupCounters = counters.getGroup(getGroupName(counters.getGroupNames()));

			Iterator<Counter> it = groupCounters.iterator();
			HashMap<Integer,Long> counterList = new HashMap<Integer, Long>();

			while(it.hasNext()) {
				try{
					Counter c = it.next();
					counterList.put(Integer.valueOf(c.getDisplayName()), c.getValue());
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
			counterSize = counterList.size();
			counterPairs = new ArrayList<Pair<String,Long>>();

			for(int i = 0; i < counterSize; i++){
				previousSum += previousValue;
				previousValue = counterList.get(Integer.valueOf(i));
				counterPairs.add(new Pair<String, Long>(TezJobControlCompiler.PIG_MAP_COUNTER + operationID + TezJobControlCompiler.PIG_MAP_SEPARATOR + i, previousSum));
			}

			globalCounters.put(operationID, counterPairs);

		} catch (Exception e) {
			String msg = "Error to read counters into Rank operation counterSize "+counterSize;
			throw new RuntimeException(msg, e);
		}
	}

	private String getGroupName(Collection<String> collection) {
		for (String name : collection) {
			if (name.contains(PIG_MAP_RANK_NAME))
				return name;
		}
		return null;
	}

	private TezOperPlan getMRRChain(TezOperPlan plan, TezOperator root) {

		TezOperPlan subDAG = new TezOperPlan();

		if (root != null) {
			subDAG.add(root);
		}

		while (root != null) {

			List<TezOperator> successors = plan.getSuccessors(root);

			if (successors != null && successors.size() == 1) {
				TezOperator successor = successors.get(0);
				if (!(plan.getEdgeProperty(root, successor) instanceof TezOperPlan.DependencyEdge)) {
					subDAG.add(successor);
					try {
						subDAG.connect(root, successor);
						subDAG.annotateEdge(root, successor, plan.getEdgeProperty(root, successor));
						root = successor; 
					} catch (PlanException e) {}
				} else {
					root = null;
				}
			} else {
				root = null;
			}
		}

		return subDAG; 
	}




	/**
	 * The method that creates the Job corresponding to a MapReduceOper.
	 * The assumption is that
	 * every MapReduceOper will have a load and a store. The JobConf removes
	 * the load operator and serializes the input filespec so that PigInputFormat can
	 * take over the creation of splits. It also removes the store operator
	 * and serializes the output filespec so that PigOutputFormat can take over
	 * record writing. The remaining portion of the map plan and reduce plans are
	 * serialized and stored for the PigMapReduce or PigMapOnly objects to take over
	 * the actual running of the plans.
	 * The Mapper &amp; Reducer classes and the required key value formats are set.
	 * Checks if this is a map only job and uses PigMapOnly class as the mapper
	 * and uses PigMapReduce otherwise.
	 * If it is a Map Reduce job, it is bound to have a package operator. Remove it from
	 * the reduce plan and serializes it so that the PigMapReduce class can use it to package
	 * the indexed tuples received by the reducer.
	 * @param mro - The MapReduceOper for which the JobConf is required
	 * @param config - the Configuration object from which JobConf is built
	 * @param pigContext - The PigContext passed on from execution engine
	 * @param mrrChain 
	 * @return Job corresponding to mro
	 * @throws JobCreationException
	 */
	@SuppressWarnings({ "unchecked", "deprecation" })
	private TezJob getJob(TezOperPlan dag, TezConfiguration conf, PigContext pigContext) throws JobCreationException{
		
		org.apache.hadoop.mapreduce.Job nwJob = null;


		try{
			nwJob = new org.apache.hadoop.mapreduce.Job(conf);
		}catch(Exception e) {
			throw new JobCreationException(e);
		}

		//Configuration conf = nwJob.getConfiguration();


		try {
			String buffPercent = conf.get("mapred.job.reduce.markreset.buffer.percent");
			if (buffPercent == null || Double.parseDouble(buffPercent) <= 0) {
				log.info("mapred.job.reduce.markreset.buffer.percent is not set, set to default 0.3");
				conf.set("mapred.job.reduce.markreset.buffer.percent", "0.3");
			}else{
				log.info("mapred.job.reduce.markreset.buffer.percent is set to " + conf.get("mapred.job.reduce.markreset.buffer.percent"));
			}

			// Convert mapred.output.* to output.compression.*, See PIG-1791
			if( "true".equals( conf.get( "mapred.output.compress" ) ) ) {
				conf.set( "output.compression.enabled",  "true" );
				String codec = conf.get( "mapred.output.compression.codec" );
				if( codec == null ) {
					throw new JobCreationException("'mapred.output.compress' is set but no value is specified for 'mapred.output.compression.codec'." );
				} else {
					conf.set( "output.compression.codec", codec );
				}
			}


			// if user specified the job name using -D switch, Pig won't reset the name then.
			if (System.getProperty("mapred.job.name") == null &&
					pigContext.getProperties().getProperty(PigContext.JOB_NAME) != null){
				conf.set("mapreduce.job.name", pigContext.getProperties().getProperty(PigContext.JOB_NAME));
				//nwJob.setJobName(pigContext.getProperties().getProperty(PigContext.JOB_NAME));
			}

			if (pigContext.getProperties().getProperty(PigContext.JOB_PRIORITY) != null) {
				// If the job priority was set, attempt to get the corresponding enum value
				// and set the hadoop job priority.
				String jobPriority = pigContext.getProperties().getProperty(PigContext.JOB_PRIORITY).toUpperCase();
				try {
					// Allow arbitrary case; the Hadoop job priorities are all upper case.
					conf.set("mapred.job.priority", JobPriority.valueOf(jobPriority).toString());

				} catch (IllegalArgumentException e) {
					StringBuffer sb = new StringBuffer("The job priority must be one of [");
					JobPriority[] priorities = JobPriority.values();
					for (int i = 0; i < priorities.length; ++i) {
						if (i > 0)  sb.append(", ");
						sb.append(priorities[i]);
					}
					sb.append("].  You specified [" + jobPriority + "]");
					throw new JobCreationException(sb.toString());
				}
			}

			nwJob.setInputFormatClass(PigInputFormat.class);
			nwJob.setOutputFormatClass(PigOutputFormat.class);
			
			conf.setClass("mapreduce.job.inputformat.class", PigInputFormat.class, InputFormat.class);
			conf.setClass("mapreduce.job.outputformat.class", PigOutputFormat.class, OutputFormat.class);

			//nwJob.setInputFormatClass(PigInputFormat.class);
			//nwJob.setOutputFormatClass(PigOutputFormat.class);

			// tmp file compression setups
			if (Utils.tmpFileCompression(pigContext)) {
				conf.setBoolean("pig.tmpfilecompression", true);
				conf.set("pig.tmpfilecompression.codec", Utils.tmpFileCompressionCodec(pigContext));
			}


			// It's a hack to set distributed cache file for hadoop 23. Once MiniMRCluster do not require local
			// jar on fixed location, this can be removed
			if (pigContext.getExecType() == ExecType.MAPREDUCE) {
				String newfiles = conf.get("alternative.mapreduce.job.cache.files");
				if (newfiles!=null) {
					String files = conf.get("mapreduce.job.cache.files");
					conf.set("mapreduce.job.cache.files",
							files == null ? newfiles.toString() : files + "," + newfiles);
				}
			}

			// Serialize the UDF specific context info.
			UDFContext.getUDFContext().serialize(conf);


			FileSystem remoteFs = FileSystem.get(conf);

			ApplicationId appId =
					tezClient.createApplication();

			Path remoteStagingDir =
					remoteFs.makeQualified(new Path(conf.get(
							TezConfiguration.TEZ_AM_STAGING_DIR,
							TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT),
							appId.toString()));
			tezClient.ensureExists(remoteStagingDir);

			DAG tezDag = createDAG(plan, remoteFs, conf, appId, remoteStagingDir);
			
			Map<String, LocalResource> amLocalResources =
					new HashMap<String, LocalResource>();
			
			amLocalResources.put("pig-tez.jar",tezDag.getVertices().get(0).getTaskLocalResources().get("pig-tez.jar"));
			amLocalResources.put("dag_job.jar", tezDag.getVertices().get(0).getTaskLocalResources().get("dag_job.jar"));

			return new TezJob(conf, appId, tezDag, remoteStagingDir, null, null, null, null, amLocalResources);

		} catch(Exception e) {
			int errCode = 2017;
			String msg = "Internal error creating job configuration.";
			throw new JobCreationException(msg, errCode, PigException.BUG, e);
		}

		//jobStoreMap.put(cjob,new Pair<List<POStore>, Path>(storeLocations, tmpLocation));

		/*
	} catch (JobCreationException jce) {
		throw jce;
	} catch(Exception e) {
		int errCode = 2017;
		String msg = "Internal error creating job configuration.";
		throw new JobCreationException(msg, errCode, PigException.BUG, e);
	}   */

	}

	public DAG createDAG(TezOperPlan tezPlan, FileSystem remoteFs, TezConfiguration conf,
			ApplicationId appId, Path remoteStagingDir)
					throws IOException, YarnException {

		DAG dag = new DAG("MRRSleepJob");
/*
		String jarPath = ClassUtil.findContainingJar(getClass());
		Path remoteJarPath = remoteFs.makeQualified(
				new Path(remoteStagingDir, "dag_job.jar"));
		remoteFs.copyFromLocalFile(new Path(jarPath), remoteJarPath);
		FileStatus jarFileStatus = remoteFs.getFileStatus(remoteJarPath);
*/
		Map<String, LocalResource> commonLocalResources =
				new HashMap<String, LocalResource>();
		
		
		if (!pigContext.inIllustrator && pigContext.getExecType() != ExecType.TEZ_LOCAL)
		{

			// Setup the DistributedCache for this job
			for (URL extraJar : pigContext.extraJars) {
				//log.debug("Adding jar to DistributedCache: " + extraJar.toString());
				TezJobControlCompiler.putJarOnClassPathThroughDistributedCache(pigContext, conf, extraJar);
			}

			//Create the jar of all functions and classes required
			File submitJarFile = File.createTempFile("Job", ".jar");
			//log.info("creating jar file "+submitJarFile.getName());
			// ensure the job jar is deleted on exit
			submitJarFile.deleteOnExit();
			FileOutputStream fos = new FileOutputStream(submitJarFile);
			try {
				JarManager.createJar(fos, new HashSet<String>(), pigContext);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			Path remoteJarPath = remoteFs.makeQualified(
					new Path(remoteStagingDir, "dag_job.jar"));
			remoteFs.copyFromLocalFile(new Path(submitJarFile.getAbsolutePath()), remoteJarPath);
			FileStatus jarFileStatus = remoteFs.getFileStatus(remoteJarPath);
			
			LocalResource dagJarLocalRsrc = LocalResource.newInstance(
					ConverterUtils.getYarnUrlFromPath(remoteJarPath),
					LocalResourceType.FILE,
					LocalResourceVisibility.APPLICATION,
					jarFileStatus.getLen(),
					jarFileStatus.getModificationTime());
			commonLocalResources.put("dag_job.jar", dagJarLocalRsrc);
			
			
			Path remoteTezJarPath = remoteFs.makeQualified(
					new Path(remoteStagingDir, "pig-tez.jar"));
			remoteFs.copyFromLocalFile(new Path("pig-tez.jar"), remoteTezJarPath);
			FileStatus tezJarFileStatus = remoteFs.getFileStatus(remoteTezJarPath);
			
			LocalResource tezJarLocalRsrc = LocalResource.newInstance(
					ConverterUtils.getYarnUrlFromPath(remoteTezJarPath),
					LocalResourceType.FILE,
					LocalResourceVisibility.APPLICATION,
					tezJarFileStatus.getLen(),
					tezJarFileStatus.getModificationTime());
			commonLocalResources.put("pig-tez.jar", tezJarLocalRsrc);
			
			
			
			//log.info("jar file "+submitJarFile.getName()+" created");
			//Start setting the JobConf properties
			conf.set("mapred.jar", submitJarFile.getPath());
		}
		
		
		/*
		LocalResource dagJarLocalRsrc = LocalResource.newInstance(
				ConverterUtils.getYarnUrlFromPath(remoteJarPath),
				LocalResourceType.FILE,
				LocalResourceVisibility.APPLICATION,
				jarFileStatus.getLen(),
				jarFileStatus.getModificationTime());
		commonLocalResources.put("dag_job.jar", dagJarLocalRsrc);
		*/

		Hashtable<TezOperator, Pair<Vertex, Configuration>> vertexMap = new Hashtable<TezOperator, Pair<Vertex, Configuration>>();

		List<TezOperator> operators = tezPlan.getRoots();


		// add settings for pig statistics
		String setScriptProp = conf.get(ScriptState.INSERT_ENABLED, "true");
		ScriptState ss = null;

		if (setScriptProp.equalsIgnoreCase("true")) {
			ss = ScriptState.get();
		}

		while (operators != null && operators.size() != 0) {
			
			List<TezOperator> successors = new ArrayList<TezOperator>(); 

			for (TezOperator oper : operators) {

				Configuration operConf = oper.configure(pigContext, conf);
				/*
				if (ss != null){
					ss.addSettingsToConf(oper, conf);
				}
				*/
				List<TezOperator> predecessors = plan.getPredecessors(oper);
				
				if (predecessors != null && predecessors.size() != 0){
					MultiStageMRConfToTezTranslator.translateVertexConfToTez(operConf, vertexMap.get(predecessors.get(0)).second);
				} else {
					MultiStageMRConfToTezTranslator.translateVertexConfToTez(operConf, null);
				}
				
				List<TezOperator> operSuccessors = tezPlan.getSuccessors(oper);
				if (operSuccessors != null){
					successors.addAll(operSuccessors);
				}

				MRHelpers.doJobClientMagic(operConf);

				//mapStageConf.setInt(MRJobConfig.NUM_MAPS, numMapper);

				Vertex operVertex = new Vertex(oper.name(), new ProcessorDescriptor(
						oper.getProcessor(),
						MRHelpers.createUserPayloadFromConf(operConf)),
						oper.getParallelism(),
						MRHelpers.getMapResource(operConf));

				oper.configureVertex(operVertex, operConf, commonLocalResources, remoteStagingDir);

				dag.addVertex(operVertex);
				if (predecessors != null) {

					for (TezOperator predecessor : predecessors) {
						dag.addEdge(new Edge(vertexMap.get(predecessor).first, operVertex, tezPlan.getEdgeProperty(predecessor, oper)));
					}

				}

				vertexMap.put(oper, new Pair<Vertex, Configuration>(operVertex, operConf));
			}
			
			operators = successors;
		}
		return dag;
	}


	/**
	 * Adjust the number of reducers based on the default_parallel, requested parallel and estimated
	 * parallel. For sampler jobs, we also adjust the next job in advance to get its runtime parallel as
	 * the number of partitions used in the sampler.
	 * @param plan the MR plan
	 * @param mro the MR operator
	 * @param nwJob the current job
	 * @throws IOException
	 */

	/*
public void adjustNumReducers(MROperPlan plan, MapReduceOper mro,
		org.apache.hadoop.mapreduce.Job nwJob) throws IOException {
	int jobParallelism = calculateRuntimeReducers(mro, nwJob);

	if (mro.isSampler()) {
		// We need to calculate the final number of reducers of the next job (order-by or skew-join)
		// to generate the quantfile.
		MapReduceOper nextMro = plan.getSuccessors(mro).get(0);

		// Here we use the same conf and Job to calculate the runtime #reducers of the next job
		// which is fine as the statistics comes from the nextMro's POLoads
		int nPartitions = calculateRuntimeReducers(nextMro, nwJob);

		// set the runtime #reducer of the next job as the #partition
		ParallelConstantVisitor visitor =
				new ParallelConstantVisitor(mro.reducePlan, nPartitions);
		visitor.visit();
	}
	log.info("Setting Parallelism to " + jobParallelism);

	Configuration conf = nwJob.getConfiguration();

	// set various parallelism into the job conf for later analysis, PIG-2779
	conf.setInt("pig.info.reducers.default.parallel", pigContext.defaultParallel);
	conf.setInt("pig.info.reducers.requested.parallel", mro.requestedParallelism);
	conf.setInt("pig.info.reducers.estimated.parallel", mro.estimatedParallelism);

	// this is for backward compatibility, and we encourage to use runtimeParallelism at runtime
	mro.requestedParallelism = jobParallelism;

	// finally set the number of reducers
	conf.setInt("mapred.reduce.tasks", jobParallelism);
}
	 */

	/**
	 * Calculate the runtime #reducers based on the default_parallel, requested parallel and estimated
	 * parallel, and save it to MapReduceOper's runtimeParallelism.
	 * @return the runtimeParallelism
	 * @throws IOException
	 */
	/*
private int calculateRuntimeReducers(MapReduceOper mro,
		org.apache.hadoop.mapreduce.Job nwJob) throws IOException{
	// we don't recalculate for the same job
	if (mro.runtimeParallelism != -1) {
		return mro.runtimeParallelism;
	}

	int jobParallelism = -1;

	if (mro.requestedParallelism > 0) {
		jobParallelism = mro.requestedParallelism;
	} else if (pigContext.defaultParallel > 0) {
		jobParallelism = pigContext.defaultParallel;
	} else {
		mro.estimatedParallelism = estimateNumberOfReducers(nwJob, mro);
		if (mro.estimatedParallelism > 0) {
			jobParallelism = mro.estimatedParallelism;
		} else {
			// reducer estimation could return -1 if it couldn't estimate
			log.info("Could not estimate number of reducers and no requested or default " +
					"parallelism set. Defaulting to 1 reducer.");
			jobParallelism = 1;
		}
	}

	// save it
	mro.runtimeParallelism = jobParallelism;
	return jobParallelism;
}
	 */
	/**
	 * Looks up the estimator from REDUCER_ESTIMATOR_KEY and invokes it to find the number of
	 * reducers to use. If REDUCER_ESTIMATOR_KEY isn't set, defaults to InputSizeReducerEstimator.
	 * @param job
	 * @param mapReducerOper
	 * @throws IOException
	 */
	/*
public static int estimateNumberOfReducers(org.apache.hadoop.mapreduce.Job job,
		MapReduceOper mapReducerOper) throws IOException {
	Configuration conf = job.getConfiguration();

	PigReducerEstimator estimator = conf.get(REDUCER_ESTIMATOR_KEY) == null ?
			new InputSizeReducerEstimator() :
				PigContext.instantiateObjectFromParams(conf,
						REDUCER_ESTIMATOR_KEY, REDUCER_ESTIMATOR_ARG_KEY, PigReducerEstimator.class);

			log.info("Using reducer estimator: " + estimator.getClass().getName());
			int numberOfReducers = estimator.estimateNumberOfReducers(job, mapReducerOper);
			return numberOfReducers;
}

public static class PigSecondaryKeyGroupComparator extends WritableComparator {
	public PigSecondaryKeyGroupComparator() {
		//            super(TupleFactory.getInstance().tupleClass(), true);
		super(NullableTuple.class, true);
	}

	@SuppressWarnings("unchecked")
	@Override
	public int compare(WritableComparable a, WritableComparable b)
	{
		PigNullableWritable wa = (PigNullableWritable)a;
		PigNullableWritable wb = (PigNullableWritable)b;
		if ((wa.getIndex() & PigNullableWritable.mqFlag) != 0) { // this is a multi-query index
			if ((wa.getIndex() & PigNullableWritable.idxSpace) < (wb.getIndex() & PigNullableWritable.idxSpace)) return -1;
			else if ((wa.getIndex() & PigNullableWritable.idxSpace) > (wb.getIndex() & PigNullableWritable.idxSpace)) return 1;
			// If equal, we fall through
		}

		// wa and wb are guaranteed to be not null, POLocalRearrange will create a tuple anyway even if main key and secondary key
		// are both null; however, main key can be null, we need to check for that using the same logic we have in PigNullableWritable
		Object valuea = null;
		Object valueb = null;
		try {
			// Get the main key from compound key
			valuea = ((Tuple)wa.getValueAsPigType()).get(0);
			valueb = ((Tuple)wb.getValueAsPigType()).get(0);
		} catch (ExecException e) {
			throw new RuntimeException("Unable to access tuple field", e);
		}
		if (!wa.isNull() && !wb.isNull()) {

			int result = DataType.compare(valuea, valueb);

			// If any of the field inside tuple is null, then we do not merge keys
			// See PIG-927
			if (result == 0 && valuea instanceof Tuple && valueb instanceof Tuple)
			{
				try {
					for (int i=0;i<((Tuple)valuea).size();i++)
						if (((Tuple)valueb).get(i)==null)
							return (wa.getIndex()&PigNullableWritable.idxSpace) - (wb.getIndex()&PigNullableWritable.idxSpace);
				} catch (ExecException e) {
					throw new RuntimeException("Unable to access tuple field", e);
				}
			}
			return result;
		} else if (valuea==null && valueb==null) {
			// If they're both null, compare the indicies
			if ((wa.getIndex() & PigNullableWritable.idxSpace) < (wb.getIndex() & PigNullableWritable.idxSpace)) return -1;
			else if ((wa.getIndex() & PigNullableWritable.idxSpace) > (wb.getIndex() & PigNullableWritable.idxSpace)) return 1;
			else return 0;
		}
		else if (valuea==null) return -1;
		else return 1;
	}
}

	 */



	public static void selectComparator(
			byte keyType,
			boolean hasOrderBy, 
			org.apache.hadoop.mapreduce.Job job) throws JobCreationException {

		if (hasOrderBy) {
			switch (keyType) {
			case DataType.BOOLEAN:
				job.setSortComparatorClass(PigBooleanRawComparator.class);
				break;

			case DataType.INTEGER:
				job.setSortComparatorClass(PigIntRawComparator.class);
				break;

			case DataType.LONG:
				job.setSortComparatorClass(PigLongRawComparator.class);
				break;

			case DataType.FLOAT:
				job.setSortComparatorClass(PigFloatRawComparator.class);
				break;

			case DataType.DOUBLE:
				job.setSortComparatorClass(PigDoubleRawComparator.class);
				break;

			case DataType.DATETIME:
				job.setSortComparatorClass(PigDateTimeRawComparator.class);
				break;

			case DataType.CHARARRAY:
				job.setSortComparatorClass(PigTextRawComparator.class);
				break;

			case DataType.BYTEARRAY:
				job.setSortComparatorClass(PigBytesRawComparator.class);
				break;

			case DataType.BIGINTEGER:
				job.setSortComparatorClass(PigBigIntegerRawComparator.class);
				break;

			case DataType.BIGDECIMAL:
				job.setSortComparatorClass(PigBigDecimalRawComparator.class);
				break;

			case DataType.MAP:
				int errCode = 1068;
				String msg = "Using Map as key not supported.";
				throw new JobCreationException(msg, errCode, PigException.INPUT);

			case DataType.TUPLE:
				job.setSortComparatorClass(PigTupleSortComparator.class);
				break;

			case DataType.BAG:
				errCode = 1068;
				msg = "Using Bag as key not supported.";
				throw new JobCreationException(msg, errCode, PigException.INPUT);

			default:
				break;
			}
			return;
		}

		switch (keyType) {
		case DataType.BOOLEAN:
			job.setSortComparatorClass(PigBooleanWritableComparator.class);
			job.setGroupingComparatorClass(PigGroupingBooleanWritableComparator.class);
			break;

		case DataType.INTEGER:
			job.setSortComparatorClass(PigIntWritableComparator.class);
			job.setGroupingComparatorClass(PigGroupingIntWritableComparator.class);
			break;

		case DataType.BIGINTEGER:
			job.setSortComparatorClass(PigBigIntegerWritableComparator.class);
			job.setGroupingComparatorClass(PigGroupingBigIntegerWritableComparator.class);
			break;

		case DataType.BIGDECIMAL:
			job.setSortComparatorClass(PigBigDecimalWritableComparator.class);
			job.setGroupingComparatorClass(PigGroupingBigDecimalWritableComparator.class);
			break;

		case DataType.LONG:
			job.setSortComparatorClass(PigLongWritableComparator.class);
			job.setGroupingComparatorClass(PigGroupingLongWritableComparator.class);
			break;

		case DataType.FLOAT:
			job.setSortComparatorClass(PigFloatWritableComparator.class);
			job.setGroupingComparatorClass(PigGroupingFloatWritableComparator.class);
			break;

		case DataType.DOUBLE:
			job.setSortComparatorClass(PigDoubleWritableComparator.class);
			job.setGroupingComparatorClass(PigGroupingDoubleWritableComparator.class);
			break;

		case DataType.DATETIME:
			job.setSortComparatorClass(PigDateTimeWritableComparator.class);
			job.setGroupingComparatorClass(PigGroupingDateTimeWritableComparator.class);
			break;

		case DataType.CHARARRAY:
			job.setSortComparatorClass(PigCharArrayWritableComparator.class);
			job.setGroupingComparatorClass(PigGroupingCharArrayWritableComparator.class);
			break;

		case DataType.BYTEARRAY:
			job.setSortComparatorClass(PigDBAWritableComparator.class);
			job.setGroupingComparatorClass(PigGroupingDBAWritableComparator.class);
			break;

		case DataType.MAP:
			int errCode = 1068;
			String msg = "Using Map as key not supported.";
			throw new JobCreationException(msg, errCode, PigException.INPUT);

		case DataType.TUPLE:
			job.setSortComparatorClass(PigTupleWritableComparator.class);
			job.setGroupingComparatorClass(PigGroupingTupleWritableComparator.class);
			break;

		case DataType.BAG:
			errCode = 1068;
			msg = "Using Bag as key not supported.";
			throw new JobCreationException(msg, errCode, PigException.INPUT);

		default:
			errCode = 2036;
			msg = "Unhandled key type " + DataType.findTypeName(keyType);
			throw new JobCreationException(msg, errCode, PigException.BUG);
		}
	}

	public static void setupDistributedCacheForJoin(PhysicalPlan plan,
			PigContext pigContext, Configuration conf) throws IOException {

		new JoinDistributedCacheVisitor(plan, pigContext, conf)
		.visit();

	}

	public static void setupDistributedCacheForUdfs(PhysicalPlan plan,
			PigContext pigContext,
			Configuration conf) throws IOException {
		new UdfDistributedCacheVisitor(plan, pigContext, conf).visit();
	}

	public static void setupDistributedCache(PigContext pigContext,
			Configuration conf,
			Properties properties, String key,
			boolean shipToCluster)
					throws IOException {
		// Set up the DistributedCache for this job
		String fileNames = properties.getProperty(key);

		if (fileNames != null) {
			String[] paths = fileNames.split(",");
			setupDistributedCache(pigContext, conf, paths, shipToCluster);
		}
	}

	public static void setupDistributedCache(PigContext pigContext,
			Configuration conf, String[] paths, boolean shipToCluster) throws IOException {
		// Turn on the symlink feature
		DistributedCache.createSymlink(conf);

		for (String path : paths) {
			path = path.trim();
			if (path.length() != 0) {
				Path src = new Path(path);

				// Ensure that 'src' is a valid URI
				URI srcURI = toURI(src);

				// Ship it to the cluster if necessary and add to the
				// DistributedCache
				if (shipToCluster) {
					Path dst =
							new Path(FileLocalizer.getTemporaryPath(pigContext).toString());
					FileSystem fs = dst.getFileSystem(conf);
					fs.copyFromLocalFile(src, dst);

					// Construct the dst#srcName uri for DistributedCache
					URI dstURI = null;
					try {
						dstURI = new URI(dst.toString() + "#" + src.getName());
					} catch (URISyntaxException ue) {
						byte errSrc = pigContext.getErrorSource();
						int errCode = 0;
						switch(errSrc) {
						case PigException.REMOTE_ENVIRONMENT:
							errCode = 6004;
							break;
						case PigException.USER_ENVIRONMENT:
							errCode = 4004;
							break;
						default:
							errCode = 2037;
							break;
						}
						String msg = "Invalid ship specification. " +
								"File doesn't exist: " + dst;
						throw new ExecException(msg, errCode, errSrc);
					}
					DistributedCache.addCacheFile(dstURI, conf);
				} else {
					DistributedCache.addCacheFile(srcURI, conf);
				}
			}
		}
	}

	public static String addSingleFileToDistributedCache(
			PigContext pigContext, Configuration conf, String filename,
			String prefix) throws IOException {

		if (!pigContext.inIllustrator && !FileLocalizer.fileExists(filename, pigContext)) {
			throw new IOException(
					"Internal error: skew join partition file "
							+ filename + " does not exist");
		}

		String symlink = filename;

		// XXX Hadoop currently doesn't support distributed cache in local mode.
		// This line will be removed after the support is added by Hadoop team.
		if (pigContext.getExecType() != ExecType.TEZ_LOCAL) {
			symlink = prefix + "_"
					+ Integer.toString(System.identityHashCode(filename)) + "_"
					+ Long.toString(System.currentTimeMillis());
			filename = filename + "#" + symlink;
			setupDistributedCache(pigContext, conf, new String[] { filename },
					false);
		}

		return symlink;
	}


	/**
	 * Ensure that 'src' is a valid URI
	 * @param src the source Path
	 * @return a URI for this path
	 * @throws ExecException
	 */
	private static URI toURI(Path src) throws ExecException {
		String pathInString = src.toString();
		String fragment = null;
		if (pathInString.contains("#")) {
			fragment = pathInString.substring(pathInString.indexOf("#"));
			pathInString = pathInString.substring(0, pathInString.indexOf("#"));
		}

		// Encode the path
		URI uri = new Path(pathInString).toUri();
		String uriEncoded = uri.toString();
		if (fragment!=null) {
			uriEncoded = uriEncoded + fragment;
		}
		try {
			return new URI(uriEncoded);
		} catch (URISyntaxException ue) {
			int errCode = 6003;
			String msg = "Invalid cache specification. " +
					"File doesn't exist: " + src;
			throw new ExecException(msg, errCode, PigException.USER_ENVIRONMENT);
		}
	}

	/**
	 * if url is not in HDFS will copy the path to HDFS from local before adding to distributed cache
	 * @param pigContext the pigContext
	 * @param conf the job conf
	 * @param url the url to be added to distributed cache
	 * @return the path as seen on distributed cache
	 * @throws IOException
	 */
	public static void putJarOnClassPathThroughDistributedCache(
			PigContext pigContext,
			Configuration conf,
			URL url) throws IOException {

		// Turn on the symlink feature
		DistributedCache.createSymlink(conf);

		// REGISTER always copies locally the jar file. see PigServer.registerJar()
		Path pathInHDFS = shipToHDFS(pigContext, conf, url);
		// and add to the DistributedCache
		DistributedCache.addFileToClassPath(pathInHDFS, conf);
		pigContext.skipJars.add(url.getPath());
	}

	/**
	 * copy the file to hdfs in a temporary path
	 * @param pigContext the pig context
	 * @param conf the job conf
	 * @param url the url to ship to hdfs
	 * @return the location where it was shipped
	 * @throws IOException
	 */
	private static Path shipToHDFS(
			PigContext pigContext,
			Configuration conf,
			URL url) throws IOException {

		String path = url.getPath();
		int slash = path.lastIndexOf("/");
		String suffix = slash == -1 ? path : path.substring(slash+1);

		Path dst = new Path(FileLocalizer.getTemporaryPath(pigContext).toUri().getPath(), suffix);
		FileSystem fs = dst.getFileSystem(conf);
		OutputStream os = fs.create(dst);
		try {
			IOUtils.copyBytes(url.openStream(), os, 4096, true);
		} finally {
			// IOUtils can not close both the input and the output properly in a finally
			// as we can get an exception in between opening the stream and calling the method
			os.close();
		}
		return dst;
	}


	private static class JoinDistributedCacheVisitor extends PhyPlanVisitor {

		private PigContext pigContext = null;

		private Configuration conf = null;

		public JoinDistributedCacheVisitor(PhysicalPlan plan,
				PigContext pigContext, Configuration conf) {
			super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
					plan));
			this.pigContext = pigContext;
			this.conf = conf;
		}

		@Override
		public void visitFRJoin(POFRJoin join) throws VisitorException {

			// XXX Hadoop currently doesn't support distributed cache in local mode.
			// This line will be removed after the support is added
			if (pigContext.getExecType() == ExecType.TEZ_LOCAL) return;

			// set up distributed cache for the replicated files
			FileSpec[] replFiles = join.getReplFiles();
			ArrayList<String> replicatedPath = new ArrayList<String>();

			FileSpec[] newReplFiles = new FileSpec[replFiles.length];

			// the first input is not replicated
			for (int i = 0; i < replFiles.length; i++) {
				// ignore fragmented file
				String symlink = "";
				if (i != join.getFragment()) {
					symlink = "pigrepl_" + join.getOperatorKey().toString() + "_"
							+ Integer.toString(System.identityHashCode(replFiles[i].getFileName()))
							+ "_" + Long.toString(System.currentTimeMillis())
							+ "_" + i;
					replicatedPath.add(replFiles[i].getFileName() + "#"
							+ symlink);
				}
				newReplFiles[i] = new FileSpec(symlink,
						(replFiles[i] == null ? null : replFiles[i].getFuncSpec()));
			}

			join.setReplFiles(newReplFiles);

			try {
				setupDistributedCache(pigContext, conf, replicatedPath
						.toArray(new String[0]), false);
			} catch (IOException e) {
				String msg = "Internal error. Distributed cache could not " +
						"be set up for the replicated files";
				throw new VisitorException(msg, e);
			}
		}

		@Override
		public void visitMergeJoin(POMergeJoin join) throws VisitorException {

			// XXX Hadoop currently doesn't support distributed cache in local mode.
			// This line will be removed after the support is added
			if (pigContext.getExecType() == ExecType.TEZ_LOCAL) return;

			String indexFile = join.getIndexFile();

			// merge join may not use an index file
			if (indexFile == null) return;

			try {
				String symlink = addSingleFileToDistributedCache(pigContext,
						conf, indexFile, "indexfile_");
				join.setIndexFile(symlink);
			} catch (IOException e) {
				String msg = "Internal error. Distributed cache could not " +
						"be set up for merge join index file";
				throw new VisitorException(msg, e);
			}
		}

		@Override
		public void visitMergeCoGroup(POMergeCogroup mergeCoGrp)
				throws VisitorException {

			// XXX Hadoop currently doesn't support distributed cache in local mode.
			// This line will be removed after the support is added
			if (pigContext.getExecType() == ExecType.TEZ_LOCAL) return;

			String indexFile = mergeCoGrp.getIndexFileName();

			if (indexFile == null) throw new VisitorException("No index file");

			try {
				String symlink = addSingleFileToDistributedCache(pigContext,
						conf, indexFile, "indexfile_mergecogrp_");
				mergeCoGrp.setIndexFileName(symlink);
			} catch (IOException e) {
				String msg = "Internal error. Distributed cache could not " +
						"be set up for merge cogrp index file";
				throw new VisitorException(msg, e);
			}
		}
	}

	private static class UdfDistributedCacheVisitor extends PhyPlanVisitor {

		private PigContext pigContext = null;
		private Configuration conf = null;

		public UdfDistributedCacheVisitor(PhysicalPlan plan,
				PigContext pigContext,
				Configuration conf) {
			super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
					plan));
			this.pigContext = pigContext;
			this.conf = conf;
		}

		@Override
		public void visitUserFunc(POUserFunc func) throws VisitorException {

			// XXX Hadoop currently doesn't support distributed cache in local mode.
			// This line will be removed after the support is added
			if (pigContext.getExecType() == ExecType.TEZ_LOCAL) return;

			// set up distributed cache for files indicated by the UDF
			String[] files = func.getCacheFiles();
			if (files == null) return;

			try {
				setupDistributedCache(pigContext, conf, files, false);
			} catch (IOException e) {
				String msg = "Internal error. Distributed cache could not " +
						"be set up for the requested files";
				throw new VisitorException(msg, e);
			}
		}
	}

	private static class ParallelConstantVisitor extends PhyPlanVisitor {

		private int rp;

		private boolean replaced = false;

		public ParallelConstantVisitor(PhysicalPlan plan, int rp) {
			super(plan, new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
					plan));
			this.rp = rp;
		}

		@Override
		public void visitConstant(ConstantExpression cnst) throws VisitorException {
			if (cnst.getRequestedParallelism() == -1) {
				Object obj = cnst.getValue();
				if (obj instanceof Integer) {
					if (replaced) {
						// sample job should have only one ConstantExpression
						throw new VisitorException("Invalid reduce plan: more " +
								"than one ConstantExpression found in sampling job");
					}
					cnst.setValue(rp);
					cnst.setRequestedParallelism(rp);
					replaced = true;
				}
			}
		}

		boolean isReplaced() { return replaced; }
	}

}
