package io.github.zhangjig.transfer.benchmark.servlet;

import io.github.zhangjig.transfer.benchmark.jdbc.DataSourceUtils;
import io.github.zhangjig.transfer.benchmark.jdbc.Transformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * 直连转账一次commit
 * 使用配置文件： direct.properties
 */
public class DirectTransformServlet extends HttpServlet {

    private static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(DirectTransformServlet.class);

    static final String DIRECT = "direct";

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
    }

    @Override
    public void service(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doService(req, resp);
    }

    private void doService(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String from = req.getParameter("from");
        String to = req.getParameter("to");
        String amount = req.getParameter("amount");

        String result = "999999";
        try (Connection conn = DataSourceUtils.getPhysicalConnection(DIRECT)) {
            conn.setAutoCommit(false);
            transfer(conn, from, amount);
            transfer(conn, to, "-" + amount);
            conn.commit();
            result = "000000";
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
        PrintWriter writer = resp.getWriter();
        writer.println("<p>" + result + "</p>");
        writer.flush();
    }

    public static void transfer(Connection conn, String acct, String amount) throws SQLException {
        Transformer.getInstance().trans(conn, acct, new BigDecimal(amount));
    }
}
