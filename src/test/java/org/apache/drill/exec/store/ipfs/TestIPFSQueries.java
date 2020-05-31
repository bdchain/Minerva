/*
 * Copyright (c) 2018-2020 Bowen Ding, Yuedong Xu, Liang Wang
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.drill.exec.store.ipfs;

import org.apache.drill.categories.IPFSStorageTest;
import org.apache.drill.categories.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SlowTest.class, IPFSStorageTest.class})
public class TestIPFSQueries extends IPFSTestBase {

  @Test
  public void testNullQuery() throws Exception {
    testBuilder()
        .sqlQuery(String.format("SELECT * FROM ipfs.`/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n#json`"))
        .unOrdered()
        .expectsNumRecords(0)
        .go();
  }
}
