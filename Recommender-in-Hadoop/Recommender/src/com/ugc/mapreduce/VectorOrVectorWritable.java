/**
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

package com.ugc.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.math.Varint;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public final class VectorOrVectorWritable implements Writable {

  private Vector uservector;
  private Vector itemvector;
  private int sig;

  public VectorOrVectorWritable() {
  }

  public Vector getUserVector() {
    return uservector;
  }

  public Vector getItemVector() {
    return itemvector;
  }

  public int getSig() {
    return sig;
  }

  public void setUser(Vector vector) {
    this.uservector = vector;
    this.itemvector = null;
    this.sig = 0;
  }

  public void setItem(Vector vector) {
	    this.uservector = null;
	    this.itemvector = vector;
	    this.sig = 1;
	  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (sig == 0) {
      out.writeInt(sig);
      VectorWritable vw = new VectorWritable(uservector);
      vw.setWritesLaxPrecision(true);
      vw.write(out);
    } else {
    	out.writeInt(sig);
        VectorWritable vw = new VectorWritable(itemvector);
        vw.setWritesLaxPrecision(true);
        vw.write(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int sig = in.readInt();
    if (sig == 0) {
      VectorWritable writable = new VectorWritable();
      writable.readFields(in);
      setUser(writable.get());
    } else {
    	VectorWritable writable = new VectorWritable();
        writable.readFields(in);
        setItem(writable.get());
    }
  }

}
