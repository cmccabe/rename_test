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

package com.cloudera;

import static org.apache.hadoop.fs.CreateFlag.CREATE;

import org.apache.hadoop.fs.CreateFlag;

import java.io.InputStream;
import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.Thread;
import java.lang.System;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.server.datanode.SimulatedFSDataset;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This tests rename behaviors in Hadoop.
 */
public class RenameTest {
  public static void main(String[] args) throws Exception {
    System.out.println("running RenameTest: tests rename " +
        "behaviors in Hadoop...\n");

    if (args.length < 1) {
      System.err.println("You must specify a single argument: the URI " +
          "of a directory to test.\n" +
          "Examples: file:///tmp, hdfs:///\n");
      System.exit(1);
    }
    String uri = args[0];
    testFileSystemRename(new URI(uri));
  }

  public static void testFileSystemRename(URI uri) throws Exception {
    FileSystem fs = FileSystem.get(uri, new Configuration());
    Path testDir = new Path(new Path(uri), "testdir");
    System.out.println("mkdir " + testDir);
    fs.mkdirs(testDir);
    Path testFile = new Path(new Path(uri), "testfile");
    System.out.println("create " + testFile);
    FSDataOutputStream fos = fs.create(testFile);
    fos.close();
    System.out.println("rename " + testFile + " -> " + testDir);
    fs.rename(testFile, testDir);
  }

  public static void testFileContextRename(URI uri) throws Exception {
    FileContext fc = FileContext.getFileContext(uri);
    Path testDir = new Path(new Path(uri), "testdir");
    System.out.println("mkdir " + testDir);
    fc.mkdir(testDir, new FsPermission((short)0755), true);
    Path testFile = new Path(new Path(uri), "testfile");
    System.out.println("create " + testFile);
    FSDataOutputStream fos = fc.create(testFile, EnumSet.of(CREATE));
    fos.close();
    System.out.println("rename " + testFile + " -> " + testDir);
    fc.rename(testFile, testDir);
  }
}
