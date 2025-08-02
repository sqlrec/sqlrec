package com.sqlrec.frontend.service;

import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SessionManager {
    private Map<THandleIdentifier, TCLIService.Client> hiveClientMap = new ConcurrentHashMap<>();
    private Map<THandleIdentifier, THandleIdentifier> operationToSessionMap = new ConcurrentHashMap<>();
    private Map<THandleIdentifier, SqlProcessor> sqlProcessorMap = new ConcurrentHashMap<>();

    public TOpenSessionResp openSession(TOpenSessionReq tOpenSessionReq) throws TException {
        TTransport transport = new TSocket("192.168.1.5", 10000, 30000);

        TProtocol protocol = new TBinaryProtocol(transport);
        TCLIService.Client client = new TCLIService.Client(protocol);

        transport.open();

        TOpenSessionResp resp = client.OpenSession(tOpenSessionReq);
        hiveClientMap.put(resp.getSessionHandle().getSessionId(), client);
        sqlProcessorMap.put(resp.getSessionHandle().getSessionId(), new SqlProcessor());

        return resp;
    }

    public TCloseSessionResp closeSession(TCloseSessionReq tCloseSessionReq) throws TException {
        TCLIService.Client client = getHiveClient(tCloseSessionReq.getSessionHandle().getSessionId());
        if(client!=null){
            hiveClientMap.remove(tCloseSessionReq.getSessionHandle().getSessionId());
            sqlProcessorMap.remove(tCloseSessionReq.getSessionHandle().getSessionId());
            return client.CloseSession(tCloseSessionReq);
        }
        return null;
    }

    public TCLIService.Client getHiveClient(THandleIdentifier sessionId){
        return hiveClientMap.get(sessionId);
    }

    public TCLIService.Client getHiveClientByOperationId(THandleIdentifier operationId){
        if(operationToSessionMap.containsKey(operationId)){
            return getHiveClient(operationToSessionMap.get(operationId));
        }
        return null;
    }

    public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq tExecuteStatementReq) throws TException {
        THandleIdentifier sessionId = tExecuteStatementReq.getSessionHandle().getSessionId();
        TCLIService.Client client = getHiveClient(sessionId);
        TExecuteStatementResp resp = client.ExecuteStatement(tExecuteStatementReq);
        operationToSessionMap.put(resp.getOperationHandle().getOperationId(), sessionId);
        return resp;
    }

    public TCancelOperationResp CancelOperation(TCancelOperationReq tCancelOperationReq) throws TException {
        THandleIdentifier operationId = tCancelOperationReq.getOperationHandle().getOperationId();
        TCLIService.Client client = getHiveClientByOperationId(operationId);
        if(client!=null){
            operationToSessionMap.remove(operationId);
            return client.CancelOperation(tCancelOperationReq);
        }
        return null;
    }

    public TCloseOperationResp CloseOperation(TCloseOperationReq tCloseOperationReq) throws TException {
        THandleIdentifier operationId = tCloseOperationReq.getOperationHandle().getOperationId();
        TCLIService.Client client = getHiveClientByOperationId(operationId);
        if(client!=null){
            operationToSessionMap.remove(operationId);
            return client.CloseOperation(tCloseOperationReq);
        }
        return null;
    }
}
