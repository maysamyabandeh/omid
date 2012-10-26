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

package com.yahoo.omid.tso.persistence;

import java.io.FileInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import com.yahoo.omid.tso.persistence.LoggerAsyncCallback.ReadRangeCallback;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.yahoo.omid.tso.persistence.LoggerException.Code;

/**
 * This class provides a backend reader based on files (perhaps over nfs)
 */
public class SyncFileBackendReader implements LogBackendReader {
    private static final Log LOG = LogFactory.getLog(SyncFileBackendReader.class);
    FileInputStream file;
    /**
     * last read index by the cursor
     */
    long index = 0;

    /**
     * @return the file size
     */
    public long init(String nfs, String fileName) throws LoggerException, FileNotFoundException {
        final String path = nfs + "/" + fileName;
        File fileRef = new File(path);
        file = new FileInputStream(fileRef);
        LOG.warn("Successfully opened the file " + fileRef + " for reading");
        return fileRef.length();
    }

    @Override
    public void readRange(long fromIndex, long toIndex, ReadRangeCallback cb) {
        System.out.println("readRange from backend : " + fromIndex + " " + toIndex);
        //1. seek to fromIndex
        while (index + 1 < fromIndex) {
            long seekStep = fromIndex - index - 1;
            try {
                long skipped = file.skip(seekStep);
                if (skipped != seekStep)
                    LOG.error("logBackend data is not ready: skiping " + seekStep + " and it skips only " + skipped);
                index += skipped;
                System.out.println("Skipping " + seekStep + " index is: " + index);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
                //TODO: react better
            }
        }
        //2. now read entries up to toIndex
        while (index < toIndex) {
            int readStep = (int)Math.min((toIndex - index), 1500);
            byte[] dst = new byte[readStep];
            try {
                System.out.println("Reading " + readStep + " index is: " + index);
                long actualRead = file.read(dst);
                if (actualRead != readStep)
                    LOG.error("logBackend data is not ready: reading " + readStep + " and it reads only " + actualRead);
                index += actualRead;
                cb.rangePartiallyRead(Code.OK, dst);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(1);
                //TODO: react better
            }
        }
        cb.rangeReadComplete(Code.OK, index);
    }
}


