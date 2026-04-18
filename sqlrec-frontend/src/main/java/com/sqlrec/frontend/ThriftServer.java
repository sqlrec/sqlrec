package com.sqlrec.frontend;

import com.sqlrec.common.config.SqlRecConfigs;
import com.sqlrec.compiler.FunctionUpdater;
import com.sqlrec.frontend.common.PrometheusMetricsUtils;
import com.sqlrec.frontend.service.TCLIServiceImpl;
import org.apache.hive.service.rpc.thrift.TCLIService;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftServer {
    private static final Logger logger = LoggerFactory.getLogger(ThriftServer.class);

    public static void main(String[] args) throws TTransportException {
        FunctionUpdater.initFunctionUpdateService();
        PrometheusMetricsUtils.initMetrics();

        TServerSocket serverTransport = new TServerSocket(SqlRecConfigs.THRIFT_SERVER_PORT.getValue());
        TCLIServiceImpl serviceImpl = new TCLIServiceImpl();

        try {
            logger.info("Thrift server is running on port {}", SqlRecConfigs.THRIFT_SERVER_PORT.getValue());
            TThreadPoolServer.Args tArgs = new TThreadPoolServer.Args(serverTransport);
            tArgs.protocolFactory(new TBinaryProtocol.Factory());

            TProcessor tprocessor = new TCLIService.Processor<TCLIService.Iface>(serviceImpl);
            tArgs.processor(tprocessor);

            TThreadPoolServer server = new TThreadPoolServer(tArgs);

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down ThriftServer...");
                server.stop();
                serviceImpl.stop();
            }));

            server.serve();
        } finally {
            if (serverTransport != null) {
                serverTransport.close();
            }
            logger.info("ThriftServer stopped");
        }
    }
}
