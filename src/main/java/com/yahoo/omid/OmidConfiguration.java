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

package com.yahoo.omid;

import org.apache.zookeeper.ZooKeeper;
import org.apache.hadoop.conf.Configuration;
import java.util.Properties;
import java.util.List;

/**
 * To retrieve configuration of omid
 * @author maysam
 *
 */
public class OmidConfiguration extends Configuration {
    Properties[] soConfs = null;
    Properties sequencerConf = null;

    public static OmidConfiguration create() {
        OmidConfiguration conf = new OmidConfiguration();
        conf.addDefaultResource("omid-site.xml");
        return conf;
    }
    
    public Properties[] getStatusOracleConfs() {
        assert(soConfs != null);
        return soConfs;
    }

    public Properties getSequencerConf() {
        assert(sequencerConf != null);
        return sequencerConf;
    }

    /**
     * Load from zk the configuration for connecting to SOs and the sequencer
     */
    public void loadServerConfs(String zkServers) {
        //zookeeper
        String sequencerIP = null;
        String sequencerPort;
        byte[] tmp;
        assert(zkServers != null);
        try{
            ZooKeeper zk = new ZooKeeper(zkServers, 
                    Integer.parseInt(System.getProperty("SESSIONTIMEOUT", Integer.toString(10000))), 
                    null);
            tmp = zk.getData("/sequencer/ip", false, null);
            sequencerIP = new String(tmp);
            tmp = zk.getData("/sequencer/port", false, null);
            sequencerPort = new String(tmp);
            System.out.println(sequencerIP + " " + sequencerPort);
            sequencerConf = new Properties();
            sequencerConf.setProperty("tso.host", sequencerIP);
            sequencerConf.setProperty("tso.port", sequencerPort);
            sequencerConf.setProperty("tso.executor.threads", "10");

            List<String> sos = zk.getChildren("/sos", false);
            System.out.println(sos);
            assert(sos.size() > 0);
            soConfs = new Properties[sos.size()];
            String host, port;
            for (int i = 0; i < sos.size(); i++) {
                //String soId = sos.get(i);
                //TODO: the sort order of children is not in ascending order
                //TODO: here I assume that the children names are numbers from 0 to n-1
                String soId = String.valueOf(i);
                tmp = zk.getData("/sos/" + soId + "/ip", false, null);
                host = new String(tmp);
                tmp = zk.getData("/sos/" + soId + "/port", false, null);
                port = new String(tmp);
                System.out.println(host + " " + port);

                soConfs[i] = new Properties();
                soConfs[i].setProperty("tso.host", host);
                soConfs[i].setProperty("tso.port", port);
                soConfs[i].setProperty("tso.executor.threads", "10");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

    }
}

