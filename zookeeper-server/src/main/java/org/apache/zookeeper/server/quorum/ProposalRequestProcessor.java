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

package org.apache.zookeeper.server.quorum;

import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.SyncRequestProcessor;
import org.apache.zookeeper.server.quorum.Leader.XidRolloverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This RequestProcessor simply forwards requests to an AckRequestProcessor and
 * SyncRequestProcessor.
 */
public class ProposalRequestProcessor implements RequestProcessor {
    private static final Logger LOG =
        LoggerFactory.getLogger(ProposalRequestProcessor.class);

    LeaderZooKeeperServer zks;

    RequestProcessor nextProcessor;

    SyncRequestProcessor syncProcessor;

    public ProposalRequestProcessor(LeaderZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        AckRequestProcessor ackProcessor = new AckRequestProcessor(zks.getLeader());/* 2、Leader将事务提议写入本地日志后， ACK事务提议  */
        syncProcessor = new SyncRequestProcessor(zks, ackProcessor);/* 1、将事务提议写入本地日志 */
    }

    /**
     * initialize this processor
     */
    public void initialize() {
        syncProcessor.start();
    }

    public void processRequest(Request request) throws RequestProcessorException {
        // LOG.warn("Ack>>> cxid = " + request.cxid + " type = " +
        // request.type + " id = " + request.sessionId);
        // request.addRQRec(">prop");


        /** In the following IF-THEN-ELSE block, we process syncs on the leader.
         * If the sync is coming from a follower, then the follower
         * handler adds it to syncHandler. Otherwise, if it is a client of
         * the leader that issued the sync command, then syncHandler won't
         * contain the handler. In this case, we add it to syncHandler, and
         * call processRequest on the next processor.
         */

        if (request instanceof LearnerSyncRequest){
            zks.getLeader().processSync((LearnerSyncRequest)request);
        } else {
            nextProcessor.processRequest(request); /* 等待大多数节点 ack事务提议 - CommitProcessor */
            if (request.getHdr() != null) {//存在事务写提议时，需要同步大多数节点 ，例如create、setData、delete
                // We need to sync and get consensus on any transactions
                try {
                    zks.getLeader().propose(request); /* 广播事务提议，放入所有Follower节点的消息队列，异步发送 */
                } catch (XidRolloverException e) {
                    throw new RequestProcessorException(e.getMessage(), e);
                }
                syncProcessor.processRequest(request);/* 事务提议写入本地文件，给自己事务ACK */
            }
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        nextProcessor.shutdown();
        syncProcessor.shutdown();
    }

}
