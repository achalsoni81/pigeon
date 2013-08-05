package com.twitter.pig.backend.hadoop.executionengine.tez;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.pig.ComparisonFunc;
import org.apache.pig.ExecType;
import org.apache.pig.LoadFunc;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.data.SchemaTupleFrontend;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.io.NullablePartitionWritable;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.mapreduce.hadoop.InputSplitInfo;
import org.apache.tez.mapreduce.hadoop.MRHelpers;
import org.apache.tez.mapreduce.processor.map.MapProcessor;

import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.DistinctCombiner;
import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.PhyPlanSetter;
import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.PigCombiner;
import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.PigInputFormat;
import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapOnly;
import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.PigSecondaryKeyComparator;
import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigGroupingPartitionWritableComparator;
import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.JobControlCompiler.PigSecondaryKeyGroupComparator;
import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.SecondaryKeyPartitioner;
import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.SkewedPartitioner;
import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.partitioners.WeightedRangePartitioner;
import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROpPlanVisitor;

public class MapOper extends TezOperator {


	public MapOper(OperatorKey k) {
		super(k);
		// TODO Auto-generated constructor stub
	}

	//The physical plan that should be executed
	//in the combine phase if one exists. Will be used
	//by the optimizer.
	public PhysicalPlan combinePlan;

	// key for the map plan
	// this is needed when the key is null to create
	// an appropriate NullableXXXWritable object
	public byte mapKeyType;

	// Indicates that there is an operator which uses endOfAllInput flag in the 
	// map plan
	boolean endOfAllInputInMap = false;


	// If true, putting an identity combine in this
	// mapreduce job will speed things up.
	boolean needsDistinctCombiner = false;

	// If true, we will use secondary key in the map-reduce job
	boolean useSecondaryKey = false;

	//The quantiles file name if globalSort is true
	String quantFile;

	// Indicates if this is a limit after a sort
	boolean limitAfterSort = false;

	//Indicates if this job is an order by job
	boolean globalSort = false;
	
	boolean combineSmallSplits = true;

	String customPartitioner = null;

	private PhysicalPlan plan;

	private boolean isMapOnly;

	private boolean isSkewedJoin;

	private boolean isGlobalSort;

	private boolean isLimitAfterSort;

	private boolean isUDFComparatorUsed;

	private boolean isEndOfAllInputSetInMap;

	private Set<String> UDFs;

	private boolean[] secondarySortOrder;

	private String skewedJoinPartitionFile;

	private boolean[] sortOrder;

	private boolean usingTypedComparator;

	@Override
	public String getProcessor() {
		return MapProcessor.class.getName();
	}

	@Override
	public TezConfiguration configure(PigContext pigContext, Configuration conf) {

		org.apache.hadoop.mapreduce.Job nwJob = null;
		try {
			nwJob = new org.apache.hadoop.mapreduce.Job(conf);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		conf = nwJob.getConfiguration();

		ArrayList<FileSpec> inp = new ArrayList<FileSpec>();
		ArrayList<List<OperatorKey>> inpTargets = new ArrayList<List<OperatorKey>>();
		ArrayList<String> inpSignatureLists = new ArrayList<String>();
		ArrayList<Long> inpLimits = new ArrayList<Long>();
		ArrayList<POStore> storeLocations = new ArrayList<POStore>();
		Path tmpLocation = null;

		conf.set("mapred.mapper.new-api", "true");

		try{

			//Process the POLoads
			List<POLoad> lds = PlanHelper.getPhysicalOperators(getPlan(), POLoad.class);

			if(lds!=null && lds.size()>0){
				for (POLoad ld : lds) {
					LoadFunc lf = ld.getLoadFunc();
					lf.setLocation(ld.getLFile().getFileName(), nwJob);
					//Store the inp filespecs
					inp.add(ld.getLFile());
				}
			}

			//adjustNumReducers(plan, mro, nwJob);

			if(lds!=null && lds.size()>0){
				for (POLoad ld : lds) {
					//Store the target operators for tuples read
					//from this input
					List<PhysicalOperator> ldSucs = getPlan().getSuccessors(ld);
					List<OperatorKey> ldSucKeys = new ArrayList<OperatorKey>();
					if(ldSucs!=null){
						for (PhysicalOperator operator2 : ldSucs) {
							ldSucKeys.add(operator2.getOperatorKey());
						}
					}
					inpTargets.add(ldSucKeys);
					inpSignatureLists.add(ld.getSignature());
					inpLimits.add(ld.getLimit());
					//Remove the POLoad from the plan
					if (!pigContext.inIllustrator)
						getPlan().remove(ld);
				}
			}

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
				JarManager.createJar(fos, getUDFs(), pigContext);
				//log.info("jar file "+submitJarFile.getName()+" created");
				//Start setting the JobConf properties
				conf.set("mapred.jar", submitJarFile.getPath());
			}
			conf.set("pig.inputs", ObjectSerializer.serialize(inp));
			conf.set("pig.inpTargets", ObjectSerializer.serialize(inpTargets));
			conf.set("pig.inpSignatures", ObjectSerializer.serialize(inpSignatureLists));
			conf.set("pig.inpLimits", ObjectSerializer.serialize(inpLimits));
			conf.set("pig.pigContext", ObjectSerializer.serialize(pigContext));
			conf.set("udf.import.list", ObjectSerializer.serialize(PigContext.getPackageImportList()));

			TezJobControlCompiler.setupDistributedCache(pigContext, nwJob.getConfiguration(), pigContext.getProperties(),
					"pig.streaming.ship.files", true);
			TezJobControlCompiler.setupDistributedCache(pigContext, nwJob.getConfiguration(), pigContext.getProperties(),
					"pig.streaming.cache.files", false);

			nwJob.setInputFormatClass(PigInputFormat.class);

			//Process POStore and remove it from the plan
			LinkedList<POStore> stores = PlanHelper.getPhysicalOperators(getPlan(), POStore.class);	       

			for (POStore st: stores) {
				storeLocations.add(st);
				StoreFuncInterface sFunc = st.getStoreFunc();
				sFunc.setStoreLocation(st.getSFile().getFileName(), nwJob);
			}

			if (stores.size() == 1) {
				//log.info("Setting up single store job");
				POStore st = stores.get(0);
				if(!pigContext.inIllustrator)
					getPlan().remove(st);
				// set out filespecs
				String outputPathString = st.getSFile().getFileName();
				if (!outputPathString.contains("://") || outputPathString.startsWith("hdfs://")) {
					conf.set("pig.streaming.log.dir",
							new Path(outputPathString, TezJobControlCompiler.LOG_DIR).toString());
				} else {
					String tmpLocationStr =  FileLocalizer
							.getTemporaryPath(pigContext).toString();
					tmpLocation = new Path(tmpLocationStr);
					conf.set("pig.streaming.log.dir",
							new Path(tmpLocation, TezJobControlCompiler.LOG_DIR).toString());
				}
				conf.set("pig.streaming.task.output.dir", outputPathString);
			}  else if (stores.size() > 0) { 
				//log.info("Setting up multi store job");
				String tmpLocationStr =  FileLocalizer
						.getTemporaryPath(pigContext).toString();
				tmpLocation = new Path(tmpLocationStr);

				boolean disableCounter = conf.getBoolean("pig.disable.counter", false);
				if (disableCounter) {
					//log.info("Disable Pig custom output counters");
				}
				int idx = 0;
				for (POStore sto: storeLocations) {
					sto.setDisableCounter(disableCounter);
					sto.setMultiStore(true);
					sto.setIndex(idx++);
				}

				conf.set("pig.streaming.log.dir",
						new Path(tmpLocation, TezJobControlCompiler.LOG_DIR).toString());
				conf.set("pig.streaming.task.output.dir", tmpLocation.toString());	            	
			}


			// store map key type
			// this is needed when the key is null to create
			// an appropriate NullableXXXWritable object
			conf.set("pig.map.keytype", ObjectSerializer.serialize(new byte[] { mapKeyType }));

			// set parent plan in all operators in map and reduce plans
			// currently the parent plan is really used only when POStream is present in the plan
			new PhyPlanSetter(getPlan()).visit();

			// this call modifies the ReplFiles names of POFRJoin operators
			// within the MR plans, must be called before the plans are
			// serialized
			TezJobControlCompiler.setupDistributedCacheForJoin(getPlan(), pigContext, conf);

			// Search to see if we have any UDFs that need to pack things into the
			// distributed cache.
			TezJobControlCompiler.setupDistributedCacheForUdfs(getPlan(), pigContext, conf);

			SchemaTupleFrontend.copyAllGeneratedToDistributedCache(pigContext, conf);

			if(isMapOnly){
				//MapOnly Job
				nwJob.setMapperClass(PigMapOnly.Map.class);
			} else{
				//Map Reduce Job
				//Process the POPackage operator and remove it from the reduce plan
				if(!combinePlan.isEmpty()){
					POPackage combPack = (POPackage)combinePlan.getRoots().get(0);
					combinePlan.remove(combPack);
					nwJob.setCombinerClass(PigCombiner.Combine.class);
					conf.set("pig.combinePlan", ObjectSerializer.serialize(combinePlan));
					conf.set("pig.combine.package", ObjectSerializer.serialize(combPack));
				} else if (needsDistinctCombiner) {
					nwJob.setCombinerClass(DistinctCombiner.Combine.class);
					//log.info("Setting identity combiner class.");
				}
				nwJob.setMapperClass(PigMapReduce.Map.class);
				if (customPartitioner != null)
					nwJob.setPartitionerClass(PigContext.resolveClassName(customPartitioner));
				setKeyConfigs(nwJob, conf, pigContext);
			}
			
			nwJob.setOutputValueClass(NullableTuple.class);


			if(isEndOfAllInputSetInMap) {
				// this is used in Map.close() to decide whether the
				// pipeline needs to be rerun one more time in the close()
				// The pipeline is rerun only if there was a stream or merge-join.
				conf.set(TezJobControlCompiler.END_OF_INP_IN_MAP, "true");
			}

			if(!pigContext.inIllustrator)
				conf.set("pig.mapPlan", ObjectSerializer.serialize(getPlan()));


			if (!pigContext.inIllustrator)
			{
				// unset inputs for POStore, otherwise, map/reduce plan will be unnecessarily deserialized
				for (POStore st: stores) { st.setInputs(null); st.setParentPlan(null);}
				conf.set(TezJobControlCompiler.PIG_MAP_STORES, ObjectSerializer.serialize(stores));
				conf.set(TezJobControlCompiler.PIG_REDUCE_STORES, ObjectSerializer.serialize(new LinkedList<POStore>()));
			}
			
			String tmp;
			long maxCombinedSplitSize = 0;
			if (!combineSmallSplits() || pigContext.getProperties().getProperty("pig.splitCombination", "true").equals("false"))
				conf.setBoolean("pig.noSplitCombination", true);
			else if ((tmp = pigContext.getProperties().getProperty("pig.maxCombinedSplitSize", null)) != null) {
				try {
					maxCombinedSplitSize = Long.parseLong(tmp);
				} catch (NumberFormatException e) {
					//log.warn("Invalid numeric format for pig.maxCombinedSplitSize; use the default maximum combined split size");
				}
			}
			if (maxCombinedSplitSize > 0)
				conf.setLong("pig.maxCombinedSplitSize", maxCombinedSplitSize);
			

			// Serialize the UDF specific context info.
			UDFContext.getUDFContext().serialize(conf);

			return new TezConfiguration(nwJob.getConfiguration());
		} catch(Exception e) {
			int errCode = 2017;
			String msg = "Internal error creating job configuration.";
			//throw new JobCreationException(msg, errCode, PigException.BUG, e);
		}
		return null;
	}		


	private boolean combineSmallSplits() {
 		return combineSmallSplits;
	}

	@Override
	public int getParallelism() {
		return 1;
	}

	public void setKeyConfigs(org.apache.hadoop.mapreduce.Job nwJob, Configuration conf, PigContext pigContext) throws Exception {
		
		if (getUseSecondaryKey()) {
			nwJob.setGroupingComparatorClass(PigSecondaryKeyGroupComparator.class);
			nwJob.setPartitionerClass(SecondaryKeyPartitioner.class);
			nwJob.setSortComparatorClass(PigSecondaryKeyComparator.class);
			nwJob.setOutputKeyClass(NullableTuple.class);
			conf.set("pig.secondarySortOrder",
					ObjectSerializer.serialize(getSecondarySortOrder()));
		}
		else
		{	

			Class<? extends WritableComparable> keyClass = HDataType.getWritableComparableTypes(mapKeyType).getClass();
			nwJob.setOutputKeyClass(keyClass);
			TezJobControlCompiler.selectComparator(mapKeyType, hasOrderBy(), nwJob);

		}

		if(isGlobalSort() || isLimitAfterSort()){
			// Only set the quantiles file and sort partitioner if we're a
			// global sort, not for limit after sort.
			if (isGlobalSort()) {
				String symlink = TezJobControlCompiler.addSingleFileToDistributedCache(
						pigContext, conf, getQuantFile(), "pigsample");
				conf.set("pig.quantilesFile", symlink);
				nwJob.setPartitionerClass(WeightedRangePartitioner.class);
			}

			if (isUDFComparatorUsed) {
				boolean usercomparator = false;
				for (String compFuncSpec : getUDFs()) {
					Class comparator = PigContext.resolveClassName(compFuncSpec);
					if(ComparisonFunc.class.isAssignableFrom(comparator)) {
						nwJob.setMapperClass(PigMapReduce.MapWithComparator.class);
						nwJob.setReducerClass(PigMapReduce.ReduceWithComparator.class);
						conf.set("pig.usercomparator", "true");
						nwJob.setOutputKeyClass(NullableTuple.class);
						nwJob.setSortComparatorClass(comparator);
						usercomparator = true;
						break;
					}
				}
				if (!usercomparator) {
					String msg = "Internal error. Can't find the UDF comparator";
					throw new IOException (msg);
				}

			} else {
				conf.set("pig.sortOrder",
						ObjectSerializer.serialize(getSortOrder()));
			}
		}

		if (isSkewedJoin()) {
			String symlink = TezJobControlCompiler.addSingleFileToDistributedCache(pigContext,
					conf, getSkewedJoinPartitionFile(), "pigdistkey");
			conf.set("pig.keyDistFile", symlink);
			nwJob.setPartitionerClass(SkewedPartitioner.class);
			nwJob.setMapperClass(PigMapReduce.MapWithPartitionIndex.class);
			nwJob.setMapOutputKeyClass(NullablePartitionWritable.class);
			nwJob.setGroupingComparatorClass(PigGroupingPartitionWritableComparator.class);
		}


		nwJob.setOutputValueClass(NullableTuple.class);
	}


	public boolean getUseSecondaryKey() {
		return useSecondaryKey;
	}

	public boolean[] getSecondarySortOrder() {
		return secondarySortOrder;
	}

	public String getQuantFile() {
		return quantFile;
	}

	public String getSkewedJoinPartitionFile() {
		return skewedJoinPartitionFile;
	}

	public boolean[] getSortOrder() {
		return sortOrder;
	}

	public boolean hasOrderBy() {

		// If this operator is involved in an order by, use the pig specific raw
		// comparators.  If it has a cogroup, we need to set the comparator class
		// to the raw comparator and the grouping comparator class to pig specific
		// raw comparators (which skip the index).  Otherwise use the hadoop provided
		// raw comparator.

		// An operator has an order by if global sort is set or if it's successor has
		// global sort set (because in that case it's the sampling job) or if
		// it's a limit after a sort.
		boolean hasOrderBy = false;
		if (isGlobalSort() || isLimitAfterSort()|| isUsingTypedComparator()) {
			hasOrderBy = true;
		} else {
			/*
			List<MapReduceOper> succs = plan.getSuccessors(mro);
			if (succs != null) {
				MapReduceOper succ = succs.get(0);
				if (succ.isGlobalSort()) hasOrderBy = true;
			}
			*/
		}

		return hasOrderBy;

	}

	public PhysicalPlan getPlan() {
		return plan;
	}

	public void setPlan(PhysicalPlan plan) {
		this.plan = plan;
	}

	public boolean isSkewedJoin() {
		return isSkewedJoin;
	}

	public void setSkewedJoin(boolean isSkewedJoin) {
		this.isSkewedJoin = isSkewedJoin;
	}

	public boolean isGlobalSort() {
		return isGlobalSort;
	}

	public void setGlobalSort(boolean isGlobalSort) {
		this.isGlobalSort = isGlobalSort;
	}

	public boolean isLimitAfterSort() {
		return isLimitAfterSort;
	}

	public void setLimitAfterSort(boolean isLimitAfterSort) {
		this.isLimitAfterSort = isLimitAfterSort;
	}

	public Set<String> getUDFs() {
		return UDFs;
	}

	public void setUDFs(Set<String> uDFs) {
		UDFs = uDFs;
	}

	public void setSecondarySortOrder(boolean[] secondarySortOrder) {
		this.secondarySortOrder = secondarySortOrder;
	}

	public void setSkewedJoinPartitionFile(String skewedJoinPartitionFile) {
		this.skewedJoinPartitionFile = skewedJoinPartitionFile;
	}

	public void setSortOrder(boolean[] sortOrder) {
		this.sortOrder = sortOrder;
	}

	public boolean isUsingTypedComparator() {
		return usingTypedComparator;
	}

	public void setUsingTypedComparator(boolean usingTypedComparator) {
		this.usingTypedComparator = usingTypedComparator;
	}

	public String getCustomPartitioner() {
		return customPartitioner;
	}

	public boolean needsDistinctCombiner() {
		return needsDistinctCombiner;
	}

	@Override
	public void configureVertex(Vertex operVertex, Configuration operConf,
			Map<String, LocalResource> commonLocalResources, Path remoteStagingDir) {
		

		InputSplitInfo inputSplitInfo;
		try {
			inputSplitInfo = MRHelpers.generateInputSplits(operConf,
					remoteStagingDir);
		} catch (InterruptedException | ClassNotFoundException | IOException e ) {
			// TODO Auto-generated catch block
			throw new TezUncheckedException("Could not generate input splits", e);
		}
		
		FileSystem remoteFs = null;
		try {
			remoteFs = FileSystem.get(operConf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		operVertex.setTaskLocationsHint(inputSplitInfo.getTaskLocationHints());
		Map<String, LocalResource> mapLocalResources =
				new HashMap<String, LocalResource>();
		mapLocalResources.putAll(commonLocalResources);
		try {
			MRHelpers.updateLocalResourcesForInputSplits(remoteFs, inputSplitInfo,
					mapLocalResources);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		operVertex.setTaskLocalResources(mapLocalResources);
		Map<String, String> mapEnv = new HashMap<String, String>();
		MRHelpers.updateEnvironmentForMRTasks(operConf, mapEnv, true);
		operVertex.setTaskEnvironment(mapEnv);
		
	}


}





