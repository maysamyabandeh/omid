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

package com.yahoo.omid.sharedlog;

/**
 * This interface must be implemented if the other depend on the pointer and want to follow its move
 * The global pointers are long but the log pointers are int
 */
public interface FollowedPointer {
    /**
     * What is the valid range starting from x till your pointer
     * returns null if there is no valid range for x
     */
    SharedLog.LogRange followRangeAfter(long x)
        throws SharedLogException;

    /**
     * check if the specified range is valid
     */
    boolean isPointerValid(long globalX);
}
