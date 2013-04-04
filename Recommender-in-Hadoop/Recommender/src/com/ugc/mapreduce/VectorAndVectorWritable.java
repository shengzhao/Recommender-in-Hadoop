/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ugc.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Varint;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public final class VectorAndVectorWritable implements Writable {

  private Vector uservector;
  private Vector itemvector;

 
  public VectorAndVectorWritable() {
		// TODO Auto-generated constructor stub
		  
	}


  public VectorAndVectorWritable(Vector user, Vector item) {
	// TODO Auto-generated constructor stub
	  this.uservector = user;
	    this.itemvector = item;
}

public Vector getUserVector() {
    return uservector;
  }
  
  public Vector getItemVector() {
	    return itemvector;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    VectorWritable vw = new VectorWritable(uservector);
    vw.setWritesLaxPrecision(true);
    vw.write(out);
    VectorWritable uw = new VectorWritable(itemvector);
    uw.setWritesLaxPrecision(true);
    uw.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    VectorWritable writable = new VectorWritable();
    writable.readFields(in);
    uservector = writable.get();
    VectorWritable otherwritable = new VectorWritable();
    otherwritable.readFields(in);
    itemvector = otherwritable.get();
  }

}