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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.io.PrintStream;

import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.NativeMapReduceOper;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PlanPrinter;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.VisitorException;

/**
 * A visitor mechanism printing out the logical plan.
 */
public class TezPrinter extends TezOpPlanVisitor {

    private PrintStream mStream = null;
    private boolean isVerbose = true;

    /**
     * @param ps PrintStream to output plan information to
     * @param plan MR plan to print
     */
    public TezPrinter(PrintStream ps, TezOperPlan plan) {
        super(plan, new DepthFirstWalker<TezOperator, TezOperPlan>(plan));
        mStream = ps;
        mStream.println("#--------------------------------------------------");
        mStream.println("#                     TEZ DAG                      ");
        mStream.println("#--------------------------------------------------");
    }

    public void setVerbose(boolean verbose) {
        isVerbose = verbose;
    }

    @Override
    public void visitTezOp(TezOperator tezOper) throws VisitorException {
        mStream.println("Tez Vertex " + tezOper.getOperatorKey().toString());
        /*
        if(mr instanceof NativeMapReduceOper) {
            mStream.println(((NativeMapReduceOper)mr).getCommandString());
            mStream.println("--------");
            mStream.println();
            return;
        } */
        
        if (tezOper instanceof MapOper) {
            mStream.println("Map Operator");
            PlanPrinter<PhysicalOperator, PhysicalPlan> printer = new PlanPrinter<PhysicalOperator, PhysicalPlan>(((MapOper) tezOper).getPlan(), mStream);
            printer.setVerbose(isVerbose);
            printer.visit();
            mStream.println("--------");
        } else if (tezOper instanceof ReduceOper) {
        	 mStream.println("Reduce Operator");
             PlanPrinter<PhysicalOperator, PhysicalPlan> printer = new PlanPrinter<PhysicalOperator, PhysicalPlan>(((ReduceOper) tezOper).getPlan(), mStream);
             printer.setVerbose(isVerbose);
             printer.visit();
             mStream.println("--------");
        }
        mStream.println("----------------");
        mStream.println("");
        /*
        mStream.println("Global sort: " + mr.isGlobalSort());
        if (mr.getQuantFile() != null) {
            mStream.println("Quantile file: " + mr.getQuantFile());
        }
        if (mr.getUseSecondaryKey())
            mStream.println("Secondary sort: " + mr.getUseSecondaryKey());
        mStream.println("----------------");
        mStream.println("");
        */
    }
}

