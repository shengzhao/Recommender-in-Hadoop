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

/*
 * <userSig, sum, userId, count>
 * sum: the number of users which tag this tag
 * count: the number of tag which be tagged by a specific user
 */
public final class UserSigWritable implements Writable {

  private String userSig;
  private Vector vector;

  public UserSigWritable() {
  }

  public UserSigWritable(String userSig, Vector vector) {
	  this.userSig = userSig;
	  this.vector = vector;
  }
  
  public UserSigWritable(UserSigWritable newuser) {
	  this.userSig = newuser.getUserSig();
	  this.vector = newuser.getVector();
  }

  public Vector getVector() {
    return vector;
  }

  public String getUserSig() {
    return userSig;
  }

  public void set(String userSig, Vector vector) {
    this.vector = vector;
    this.userSig = userSig;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    
    VectorWritable vw = new VectorWritable(vector);
    vw.setWritesLaxPrecision(true);
    vw.write(out);
    //out.writeBytes(userSig);
    out.writeUTF(userSig);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    
    VectorWritable writable = new VectorWritable();
    writable.readFields(in);
    
    String userSig = in.readUTF();
    set(userSig, writable.get());
  }

}
