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

import com.yahoo.omid.client.TSOClient.Result;
import com.yahoo.omid.tso.RowKey;
import java.util.ArrayList;

public class SyncCommitCallback extends SyncCallbackBase implements CommitCallback {
    private Result result;
    private long commitTimestamp;
    private ArrayList<RowKey> rowsWithWriteWriteConflict;//rows with write-write conflict

    public Result getResult() {
        return result;
    }

    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    public boolean isElder() {
        return rowsWithWriteWriteConflict != null && rowsWithWriteWriteConflict.size() != 0;
    }

    public ArrayList<RowKey> getWWRows() {
        return rowsWithWriteWriteConflict;
    }

    synchronized public void complete(Result res, long commitTimestamp, ArrayList<RowKey> rowsWithWriteWriteConflict) {
        this.result = res;
        this.commitTimestamp = commitTimestamp;
        this.rowsWithWriteWriteConflict = rowsWithWriteWriteConflict;
        countDown();
    }
}

