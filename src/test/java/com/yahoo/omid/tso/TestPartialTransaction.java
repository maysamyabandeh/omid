/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

import com.yahoo.omid.tso.RowKey;
import com.yahoo.omid.tso.messages.AbortedTransactionReport;
import com.yahoo.omid.tso.messages.CommitRequest;
import com.yahoo.omid.tso.messages.CommitResponse;
import com.yahoo.omid.tso.messages.CommittedTransactionReport;
import com.yahoo.omid.tso.messages.FullAbortReport;
import com.yahoo.omid.tso.messages.TimestampRequest;
import com.yahoo.omid.tso.messages.TimestampResponse;
import com.yahoo.omid.tso.messages.PrepareCommit;
import com.yahoo.omid.tso.messages.PrepareResponse;

public class TestPartialTransaction extends TSOTestBase {

   @Test
   public void testConflicts() throws IOException, InterruptedException {
      clientHandler.sendMessage(new TimestampRequest());
      clientHandler.receiveBootstrap();
      TimestampResponse tr1 = clientHandler.receiveMessage(TimestampResponse.class);

      clientHandler.sendMessage(new TimestampRequest());
      TimestampResponse tr2 = clientHandler.receiveMessage(TimestampResponse.class);

      RowKey [] writtenRows = new RowKey[] { r1 };
      clientHandler.sendMessage(new PrepareCommit(tr1.timestamp, writtenRows));
      PrepareResponse pr1 = clientHandler.receiveMessage(PrepareResponse.class);
      assertTrue(pr1.committed);
      assertEquals(tr1.timestamp, pr1.startTimestamp);

      clientHandler.sendMessage(new CommitRequest(tr2.timestamp, new RowKey[] { r1, r2 }));
      CommitResponse cr2 = clientHandler.receiveMessage(CommitResponse.class);
      assertEquals(tr2.timestamp, cr2.startTimestamp);
      assertFalse(cr2.committed);

      CommitRequest cmtrqst = new CommitRequest(tr1.timestamp, writtenRows);
      cmtrqst.prepared = true;
      cmtrqst.successfulPrepared = pr1.committed;
      clientHandler.sendMessage(cmtrqst);
      CommitResponse cr1 = clientHandler.receiveMessage(CommitResponse.class);
      assertTrue(cr1.committed);
      assertEquals(tr1.timestamp, cr1.startTimestamp);
   }

}

