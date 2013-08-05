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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.ExecType;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.AccumulatorOptimizer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.CombinerOptimizer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.JobCreationException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.KeyTypeDiscoveryVisitor;
import org.apache.pig.backend.hadoop.executionengine.Launcher;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.LimitAdjuster;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler.LastInputStreamingOptimizer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.NoopFilterRemover;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.NoopStoreRemover;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.SampleOptimizer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.SecondaryKeyOptimizer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.EndOfAllInputSetter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.POPackageAnnotator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MultiQueryOptimizer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.ConfigurationValidator;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.ScriptState;
import org.apache.tez.client.LocalTezClient;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.TezConfiguration;


/**
 * Main class that launches pig for Map Reduce
 *
 */
public class TezLauncher extends Launcher{

    public static final String SUCCEEDED_FILE_NAME = "_SUCCESS";
    
    public static final String SUCCESSFUL_JOB_OUTPUT_DIR_MARKER =
        "mapreduce.fileoutputcommitter.marksuccessfuljobs";

    private static final Log log = LogFactory.getLog(TezLauncher.class);
 
    //used to track the exception thrown by the job control which is run in a separate thread
    private Exception jobControlException = null;
    private String jobControlExceptionStackTrace = null;
    private boolean aggregateWarning = false;

    private Map<FileSpec, Exception> failureMap;
    
    private JobControl jc=null;

	private TezClient tezClient;
    
    private class HangingJobKiller extends Thread {
        public HangingJobKiller() {
        }
        @Override
        public void run() {
            try {
                log.debug("Receive kill signal");
                if (jc!=null) {
                    for (Job job : jc.getRunningJobs()) {
                        RunningJob runningJob = job.getJobClient().getJob(job.getAssignedJobID());
                        if (runningJob!=null)
                            runningJob.killJob();
                        log.info("Job " + job.getJobID() + " killed");
                    }
                }
            } catch (Exception e) {
                log.warn("Encounter exception on cleanup:" + e);
            }
        }
    }

    public TezLauncher() {
        Runtime.getRuntime().addShutdownHook(new HangingJobKiller());
    }
    
    /**
     * Get the exception that caused a failure on the backend for a
     * store location (if any).
     */
    public Exception getError(FileSpec spec) {
        return failureMap.get(spec);
    }

    @Override
    public void reset() {  
        failureMap = new HashMap<FileSpec, Exception>();
        super.reset();
    }
   
    @SuppressWarnings("deprecation")
    @Override
    public PigStats launchPig(PhysicalPlan php,
                              String grpName,
                              PigContext pc) throws PlanException,
                                                    VisitorException,
                                                    IOException,
                                                    ExecException,
                                                    JobCreationException,
                                                    Exception {
        long sleepTime = 500;
        aggregateWarning = "true".equalsIgnoreCase(pc.getProperties().getProperty("aggregate.warning"));
        TezOperPlan tezPlan = compile(php, pc);
                
        ConfigurationValidator.validatePigProperties(pc.getProperties());
        TezConfiguration conf = new TezConfiguration(ConfigurationUtil.toConfiguration(pc.getProperties()));
        ExecutionEngine exe = pc.getExecutionEngine();
        
	    conf.set("tez.lib.uris", "hdfs://localhost:9000/tez_jars,hdfs://localhost:9000/tez_jars/lib");

        if (pc.getExecType() == ExecType.TEZ_LOCAL) {
            tezClient = new LocalTezClient(conf);
        } else {
            tezClient = new TezClient(conf);
        }

        TezJobControlCompiler tjcc = new TezJobControlCompiler(pc, conf, tezClient);
        
        
        /*
        // start collecting statistics
        PigStatsUtil.startCollection(pc, jobClient, jcc, mrp); 
        
        */
        
        // Find all the intermediate data stores. The plan will be destroyed during compile/execution
        // so this needs to be done before.
        
        //MRIntermediateDataVisitor intermediateVisitor = new MRIntermediateDataVisitor(mrp);
        //intermediateVisitor.visit();
        
        List<TezJob> failedJobs = new LinkedList<TezJob>();
        //List<NativeMapReduceOper> failedNativeMR = new LinkedList<NativeMapReduceOper>();
        List<TezJob> completeFailedJobsInThisRun = new LinkedList<TezJob>();
        List<TezJob> succJobs = new LinkedList<TezJob>();
        
        
        while(tezPlan.size() != 0) {
            ArrayList<TezJob> jobsThisRun = tjcc.compile(tezPlan, grpName);
            for (TezJob job : jobsThisRun) {
            	job.submitJob(tezClient);
            	log.info("done");
            }
            //TezSubmitter submitter = new TezSubmitter(this, jobsThisRun, conf, 2);
            //submitter.start();
            //submitter.stop();
            /*
            for (TezJob job : jobsThisRun) {
            	TezSubmitter jobSubmitter = new TezSubmitter(this, job);
            	jobSubmitter.submitJob();
            	log.info("done");
            }
            */
        }
		return null;

    }

    /**
     * If stop_on_failure is enabled and any job has failed, an ExecException is thrown.
     * @param stop_on_failure whether it's enabled.
     * @throws ExecException If stop_on_failure is enabled and any job is failed
     */
    private void checkStopOnFailure(boolean stop_on_failure) throws ExecException{
    	if (jc.getFailedJobs().isEmpty())
            return;
    	
    	if (stop_on_failure){
            int errCode = 6017;
            StringBuilder msg = new StringBuilder();
            
            for (int i=0; i<jc.getFailedJobs().size(); i++) {
                Job j = jc.getFailedJobs().get(i);
                msg.append(j.getMessage());
                if (i!=jc.getFailedJobs().size()-1) {
                    msg.append("\n");
                }
            }
            
            throw new ExecException(msg.toString(), errCode,
                    PigException.REMOTE_ENVIRONMENT);
        }
    }
    
    private String getStackStraceStr(Throwable e) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        e.printStackTrace(ps);
        return baos.toString();
    }

    /**
     * Log the progress and notify listeners if there is sufficient progress 
     * @param prog current progress
     * @param lastProg progress last time
     */
    private boolean notifyProgress(double prog, double lastProg) {
        if (prog >= (lastProg + 0.04)) {
            int perCom = (int)(prog * 100);
            if(perCom!=100) {
                log.info( perCom + "% complete");
                ScriptState.get().emitProgressUpdatedNotification(perCom);
            }
            return true;
        }
        return false;
    }

    @Override
    public void explain(
            PhysicalPlan php,
            PigContext pc,
            PrintStream ps,
            String format,
            boolean verbose) throws PlanException, VisitorException,
                                   IOException {
        log.trace("Entering MapReduceLauncher.explain");
        TezOperPlan mrp = compile(php, pc);

        if (format.equals("text")) {
            TezPrinter printer = new TezPrinter(ps, mrp);
            printer.setVerbose(verbose);
            printer.visit();
        } else {
            ps.println("#--------------------------------------------------");
            ps.println("# Map Reduce Plan                                  ");
            ps.println("#--------------------------------------------------");
            
            //DotMRPrinter printer = new DotMRPrinter(mrp, ps);
            //printer.setVerbose(verbose);
            //printer.dump();
            //ps.println("");
        }
    }

    public TezOperPlan compile(
            PhysicalPlan php,
            PigContext pc) throws PlanException, IOException, VisitorException {
        MRCompiler comp = new MRCompiler(php, pc);
        comp.randomizeFileLocalizer();
        comp.compile();
        comp.aggregateScalarsFiles();
        MROperPlan plan = comp.getMRPlan();
        
        //display the warning message(s) from the MRCompiler
        comp.getMessageCollector().logMessages(MessageType.Warning, aggregateWarning, log);
        
        String lastInputChunkSize = 
            pc.getProperties().getProperty(
                    "last.input.chunksize", POJoinPackage.DEFAULT_CHUNK_SIZE);
        
        String prop = pc.getProperties().getProperty(PigConfiguration.PROP_NO_COMBINER);
        if (!pc.inIllustrator && !("true".equals(prop)))  {
            boolean doMapAgg = 
                    Boolean.valueOf(pc.getProperties().getProperty(PigConfiguration.PROP_EXEC_MAP_PARTAGG,"false"));
            CombinerOptimizer co = new CombinerOptimizer(plan, doMapAgg);
            co.visit();
            //display the warning message(s) from the CombinerOptimizer
            co.getMessageCollector().logMessages(MessageType.Warning, aggregateWarning, log);
        }
        
        // Optimize the jobs that have a load/store only first MR job followed
        // by a sample job.
        SampleOptimizer so = new SampleOptimizer(plan, pc);
        so.visit();
        
        // We must ensure that there is only 1 reducer for a limit. Add a single-reducer job.
        if (!pc.inIllustrator) {
        LimitAdjuster la = new LimitAdjuster(plan, pc);
        la.visit();
        la.adjust();
        }
        // Optimize to use secondary sort key if possible
        prop = pc.getProperties().getProperty("pig.exec.nosecondarykey");
        if (!pc.inIllustrator && !("true".equals(prop)))  {
            SecondaryKeyOptimizer skOptimizer = new SecondaryKeyOptimizer(plan);
            skOptimizer.visit();
        }
        
        // optimize key - value handling in package
        POPackageAnnotator pkgAnnotator = new POPackageAnnotator(plan);
        pkgAnnotator.visit();
        
        // optimize joins
        LastInputStreamingOptimizer liso = 
            new MRCompiler.LastInputStreamingOptimizer(plan, lastInputChunkSize);
        liso.visit();
        
        // figure out the type of the key for the map plan
        // this is needed when the key is null to create
        // an appropriate NullableXXXWritable object
        KeyTypeDiscoveryVisitor kdv = new KeyTypeDiscoveryVisitor(plan);
        kdv.visit();

        // removes the filter(constant(true)) operators introduced by
        // splits.
        NoopFilterRemover fRem = new NoopFilterRemover(plan);
        fRem.visit();
        
        boolean isMultiQuery = 
            "true".equalsIgnoreCase(pc.getProperties().getProperty("opt.multiquery","true"));
        
        if (isMultiQuery) {
            // reduces the number of MROpers in the MR plan generated 
            // by multi-query (multi-store) script.
            MultiQueryOptimizer mqOptimizer = new MultiQueryOptimizer(plan, pc.inIllustrator);
            mqOptimizer.visit();
        }
        
        // removes unnecessary stores (as can happen with splits in
        // some cases.). This has to run after the MultiQuery and
        // NoopFilterRemover.
        NoopStoreRemover sRem = new NoopStoreRemover(plan);
        sRem.visit();
      
        // check whether stream operator is present
        // after MultiQueryOptimizer because it can shift streams from
        // map to reduce, etc.
        EndOfAllInputSetter checker = new EndOfAllInputSetter(plan);
        checker.visit();
        
        boolean isAccum = 
            "true".equalsIgnoreCase(pc.getProperties().getProperty("opt.accumulator","true"));
        if (isAccum) {
            AccumulatorOptimizer accum = new AccumulatorOptimizer(plan);
            accum.visit();
        }
        
        // splits MR plan into Tez Operator Plan consisting of Map and Reduce Opers
    	MRTezConverter mrToTezConverter = new MRTezConverter(plan);
    	mrToTezConverter.visit();
    	TezOperPlan tezPlan = mrToTezConverter.getTezPlan();
    	
    	// optimizes Tez Operator Plan to collapse multiple jobs into MRR* chains
    	MRROptimizer mrr = new MRROptimizer(tezPlan);
        mrr.visit();
        
        // annotates the type of edges between the operators to be used by TJCC
        TezEdgeAnnotator annotator = new TezEdgeAnnotator(tezPlan); 
        annotator.visit();
        
        return tezPlan;
    }

    private boolean shouldMarkOutputDir(Job job) {
        return job.getJobConf().getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, 
                               false);
    }
    
    private void createSuccessFile(Job job, POStore store) throws IOException {
        if(shouldMarkOutputDir(job)) {            
            Path outputPath = new Path(store.getSFile().getFileName());
            FileSystem fs = outputPath.getFileSystem(job.getJobConf());
            if(fs.exists(outputPath)){
                // create a file in the folder to mark it
                Path filePath = new Path(outputPath, SUCCEEDED_FILE_NAME);
                if(!fs.exists(filePath)) {
                    fs.create(filePath).close();
                }
            }    
        } 
    }
    
    /**
     * An exception handler class to handle exceptions thrown by the job controller thread
     * Its a local class. This is the only mechanism to catch unhandled thread exceptions
     * Unhandled exceptions in threads are handled by the VM if the handler is not registered
     * explicitly or if the default handler is null
     */
    class JobControlThreadExceptionHandler implements Thread.UncaughtExceptionHandler {
        
        @Override
        public void uncaughtException(Thread thread, Throwable throwable) {
            jobControlExceptionStackTrace = getStackStraceStr(throwable);
            try {	
                jobControlException = getExceptionFromString(jobControlExceptionStackTrace);
            } catch (Exception e) {
                String errMsg = "Could not resolve error that occured when launching map reduce job: "
                        + jobControlExceptionStackTrace;
                jobControlException = new RuntimeException(errMsg, throwable);
            }
        }
    }
    
    @SuppressWarnings("deprecation")
    void computeWarningAggregate(Job job, JobClient jobClient, Map<Enum, Long> aggMap) {
        JobID mapRedJobID = job.getAssignedJobID();
        RunningJob runningJob = null;
        try {
            runningJob = jobClient.getJob(mapRedJobID);
            if(runningJob != null) {
                Counters counters = runningJob.getCounters();
                if (counters==null)
                {
                    long nullCounterCount = aggMap.get(PigWarning.NULL_COUNTER_COUNT)==null?0 : aggMap.get(PigWarning.NULL_COUNTER_COUNT);
                    nullCounterCount++;
                    aggMap.put(PigWarning.NULL_COUNTER_COUNT, nullCounterCount);
                }
                try {
                    for (Enum e : PigWarning.values()) {
                        if (e != PigWarning.NULL_COUNTER_COUNT) {
                            Long currentCount = aggMap.get(e);
                            currentCount = (currentCount == null ? 0 : currentCount);
                            // This code checks if the counters is null, if it is,
                            // we need to report to the user that the number
                            // of warning aggregations may not be correct. In fact,
                            // Counters should not be null, it is
                            // a hadoop bug, once this bug is fixed in hadoop, the
                            // null handling code should never be hit.
                            // See Pig-943
                            if (counters != null)
                                currentCount += counters.getCounter(e);
                            aggMap.put(e, currentCount);
                        }
                    }
                } catch (Exception e) {
                    log.warn("Exception getting counters.", e);
                }
            }
        } catch (IOException ioe) {
            String msg = "Unable to retrieve job to compute warning aggregation.";
            log.warn(msg);
        }    	
    }

	public TezClient getTezClient() {
		return tezClient;
	}

}
