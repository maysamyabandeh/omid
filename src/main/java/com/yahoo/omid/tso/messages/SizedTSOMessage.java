package com.yahoo.omid.tso.messages;

import java.io.DataOutputStream;
import java.io.IOException;

import org.jboss.netty.buffer.ChannelBuffer;

import com.yahoo.omid.tso.TSOMessage;

/**
 * A TSOMessage that implements the size() function
 * 
 * @author maysam
 */
public abstract class SizedTSOMessage implements TSOMessage {

   protected int sizeInBytes = -1;

   @Override
   public int size() throws Exception {
       if (sizeInBytes == -1)
           throw new Exception("Message size is not set");
       return sizeInBytes;
   }

   @Override
   public void setSize(int sizeInBytes) {
       this.sizeInBytes = sizeInBytes;
   }

}

