package com.sqlrec.frontend;

import com.sqlrec.frontend.service.TCLIServiceImpl;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

public class ThriftServer {
    public static void main(String[] args) throws TTransportException {
        TServerSocket serverTransport = new TServerSocket(8000);
        TThreadPoolServer.Args tArgs = new TThreadPoolServer.Args(serverTransport);
        tArgs.protocolFactory(new TBinaryProtocol.Factory());

        TProcessor tprocessor = new TCLIService.Processor<TCLIService.Iface>(new TCLIServiceImpl());
        tArgs.processor(tprocessor);

        TThreadPoolServer server = new TThreadPoolServer(tArgs);
        server.serve();
    }
}
