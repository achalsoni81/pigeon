package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.HDataType;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce.Reduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.POStatus;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.NullableTuple;
import org.apache.pig.impl.io.PigNullableWritable;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.SpillableMemoryManager;
import org.apache.pig.tools.pigstats.PigStatusReporter;
import org.joda.time.DateTimeZone;

public class PigIntermediateReduce extends  PigMapReduce.Reduce {
	
	

    protected byte keyType;


    public void collect(Context oc, Tuple tuple) 
            throws InterruptedException, IOException {
        
        Byte index = (Byte)tuple.get(0);
        PigNullableWritable key =
            HDataType.getWritableComparableTypes(tuple.get(1), keyType);
        NullableTuple val = new NullableTuple((Tuple)tuple.get(2));
        
        // Both the key and the value need the index.  The key needs it so
        // that it can be sorted on the index in addition to the key
        // value.  The value needs it so that POPackage can properly
        // assign the tuple to its slot in the projection.
        key.setIndex(index);
        val.setIndex(index);

        oc.write(key, val);
    }
    
    
	protected void setup(Context context) throws IOException, InterruptedException {
       super.setup(context);
       Configuration jConf = context.getConfiguration();
       keyType = ((byte[])ObjectSerializer.deserialize(jConf.get("pig.map.keytype")))[0];
    }
    
	
	public boolean processOnePackageOutput(Context oc) 
            throws IOException, InterruptedException {

        Result res = pack.getNextTuple();
        if(res.returnStatus==POStatus.STATUS_OK){
            Tuple packRes = (Tuple)res.result;
            
            if(rp.isEmpty()){
                collect(outputCollector, packRes);
                return false;
            }
            for (int i = 0; i < roots.length; i++) {
                roots[i].attachInput(packRes);
            }
            runPipeline(leaf);
            
        }
        
        if(res.returnStatus==POStatus.STATUS_NULL) {
            return false;
        }
        
        if(res.returnStatus==POStatus.STATUS_ERR){
            int errCode = 2093;
            String msg = "Encountered error in package operator while processing group.";
            throw new ExecException(msg, errCode, PigException.BUG);
        }
        
        if(res.returnStatus==POStatus.STATUS_EOP) {
            return true;
        }
            
        return false;
        
    }
	
	protected void runPipeline(PhysicalOperator leaf) 
            throws InterruptedException, IOException {
        
        while(true)
        {
            Result redRes = leaf.getNextTuple();
            if(redRes.returnStatus==POStatus.STATUS_OK){
                try{
                    collect(outputCollector, (Tuple)redRes.result);
                }catch(Exception e) {
                    throw new IOException(e);
                }
                continue;
            }
            
            if(redRes.returnStatus==POStatus.STATUS_EOP) {
                return;
            }
            
            if(redRes.returnStatus==POStatus.STATUS_NULL) {
                continue;
            }
            
            if(redRes.returnStatus==POStatus.STATUS_ERR){
                // remember that we had an issue so that in 
                // close() we can do the right thing
                errorInReduce   = true;
                // if there is an errmessage use it
                String msg;
                if(redRes.result != null) {
                    msg = "Received Error while " +
                    "processing the reduce plan: " + redRes.result;
                } else {
                    msg = "Received Error while " +
                    "processing the reduce plan.";
                }
                int errCode = 2090;
                throw new ExecException(msg, errCode, PigException.BUG);
            }
        }
    }
    
}
