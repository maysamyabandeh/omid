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

package com.yahoo.omid.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestSharedAbortedSet {
    @Test
        public void testSimpleOps() throws Exception {
            SharedAbortedSet aborted = new SharedAbortedSet();
            aborted.add(12l);
            assertTrue("The reported abort is lost", aborted.contains(12l));
            aborted.remove(12l);
            assertFalse("The reported full abort is lost", aborted.contains(12l));
            aborted.add(13l);
            assertTrue("The reported abort is lost", aborted.contains(13l));
            aborted.aTxnStarted(25l);
            aborted.remove(13l);
            assertTrue("The full abort changes the read snapshot of a running transaction", aborted.contains(13l));
            aborted.add(14l);
            assertTrue("The reported abort is lost", aborted.contains(14l));
            aborted.aTxnFinished(25l);
            assertFalse("The full abort is lost, even after commit of the outstanding transaction", aborted.contains(13l));
            assertTrue("The reported abort is lost", aborted.contains(14l));
            aborted.remove(14l);
            assertFalse("The full abort is lost, although there is no outstanding transaction", aborted.contains(14l));
        }

    @Test
        public void testComplexSequences() throws Exception {
            SharedAbortedSet aborted = new SharedAbortedSet();
            aborted.add(12l);
            aborted.add(13l);
            aborted.add(14l);
            aborted.add(15l);
            aborted.add(16l);
            aborted.add(17l);
            aborted.aTxnStarted(25l);
            aborted.remove(12l);
            aborted.aTxnStarted(26l);
            aborted.remove(13l);
            aborted.remove(14l);
            aborted.remove(15l);
            aborted.aTxnStarted(27l);
            aborted.remove(16l);
            aborted.aTxnStarted(28l);
            aborted.remove(17l);
            assertTrue("The reported abort is lost", aborted.contains(12l));
            assertTrue("The reported abort is lost", aborted.contains(13l));
            assertTrue("The reported abort is lost", aborted.contains(14l));
            assertTrue("The reported abort is lost", aborted.contains(15l));
            assertTrue("The reported abort is lost", aborted.contains(16l));
            assertTrue("The reported abort is lost", aborted.contains(17l));
            assertTrue("The reported abort is lost", aborted.contains(12l));
            assertTrue("The reported abort is lost", aborted.contains(13l));
            assertTrue("The reported abort is lost", aborted.contains(14l));
            assertTrue("The reported abort is lost", aborted.contains(15l));
            assertTrue("The reported abort is lost", aborted.contains(16l));
            assertTrue("The reported abort is lost", aborted.contains(17l));
            aborted.aTxnFinished(25l);
            assertFalse("The reported full abort is lost", aborted.contains(12l));
            assertTrue("The reported abort is lost", aborted.contains(13l));
            assertTrue("The reported abort is lost", aborted.contains(14l));
            assertTrue("The reported abort is lost", aborted.contains(15l));
            assertTrue("The reported abort is lost", aborted.contains(16l));
            assertTrue("The reported abort is lost", aborted.contains(17l));
            aborted.aTxnFinished(26l);
            assertFalse("The reported full abort is lost", aborted.contains(12l));
            assertFalse("The reported full abort is lost", aborted.contains(13l));
            assertFalse("The reported full abort is lost", aborted.contains(14l));
            assertFalse("The reported full abort is lost", aborted.contains(15l));
            assertTrue("The reported abort is lost", aborted.contains(16l));
            assertTrue("The reported abort is lost", aborted.contains(17l));
            aborted.aTxnFinished(28l);
            assertFalse("The reported full abort is lost", aborted.contains(12l));
            assertFalse("The reported full abort is lost", aborted.contains(13l));
            assertFalse("The reported full abort is lost", aborted.contains(14l));
            assertFalse("The reported full abort is lost", aborted.contains(15l));
            assertTrue("The reported abort is lost", aborted.contains(16l));
            assertTrue("The reported abort is lost", aborted.contains(17l));
            aborted.aTxnFinished(27l);
            assertFalse("The reported full abort is lost", aborted.contains(12l));
            assertFalse("The reported full abort is lost", aborted.contains(13l));
            assertFalse("The reported full abort is lost", aborted.contains(14l));
            assertFalse("The reported full abort is lost", aborted.contains(15l));
            assertFalse("The reported full abort is lost", aborted.contains(16l));
            assertFalse("The reported full abort is lost", aborted.contains(17l));
        }
}

