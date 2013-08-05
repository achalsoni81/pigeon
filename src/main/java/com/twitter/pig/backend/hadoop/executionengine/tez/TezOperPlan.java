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
package com.twitter.pig.backend.hadoop.executionengine.tez;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Hashtable;

import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.ConnectionPattern;

import com.twitter.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceOper;


/**
 * A Plan used to create the plan of 
 * Map Reduce Operators which can be 
 * converted into the Job Control
 * object. This is necessary to capture
 * the dependencies among jobs
 */
public class TezOperPlan extends OperatorPlan<TezOperator> {
	
	
	public class DependencyEdge extends EdgeProperty {
		
		public DependencyEdge() {
			super(null, null, null, null);
		}
	}

	private static final long serialVersionUID = 1L;
	private Hashtable<Pair<TezOperator, TezOperator>, EdgeProperty> annotatedEdges = new Hashtable<Pair<TezOperator, TezOperator>, EdgeProperty>();

	public TezOperPlan() {
		// TODO Auto-generated constructor stub
	}
	
	
	public void annotateEdge(TezOperator src, TezOperator dst, EdgeProperty edgeProperty) {
		annotatedEdges.put(new Pair<TezOperator, TezOperator>(src, dst), edgeProperty);
	}
	
	public EdgeProperty getEdgeProperty(TezOperator src, TezOperator dst) {
		return annotatedEdges.get(new Pair<TezOperator, TezOperator>(src, dst));
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(baos);
		TezPrinter printer = new TezPrinter(ps, this);
		printer.setVerbose(true);
		try {
			printer.visit();
		} catch (VisitorException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException("Unable to get String representation of plan:" + e );
		}
		return baos.toString();
	}




}
