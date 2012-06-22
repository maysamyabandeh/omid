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

import com.yahoo.omid.tso.TSOMessage;
import java.util.concurrent.CountDownLatch;

public class PingMultiPongCallback<PONG extends TSOMessage> extends PingCallback {
    PONG[] pongs;
    int receivedPongs = 0;

    PingMultiPongCallback(int pongsCnt) {
        //pongs = new PONG[pongsCnt];
        pongs = (PONG[]) new TSOMessage[pongsCnt];
        latch = new CountDownLatch(pongsCnt);
    }

    public PONG[] getPongs() {
        return pongs;
    }

    synchronized public void complete(PONG pong) {
        pongs[receivedPongs] = pong;
        receivedPongs++;
        complete();
    }

    synchronized public void error(Exception e) {
        this.e = e;
        while (latch.getCount() > 0)
            countDown();
    }

}

