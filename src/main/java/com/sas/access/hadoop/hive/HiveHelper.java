/* $Id: HiveHelper.java,v 1.1.4.3.2.3.4.5.2.4.4.3 2015/09/03 15:47:56 dazant Exp $ */
/*-----------------------------------------------------------------------------
 *  Copyright (C) 2011-2015 by SAS Institute Inc., Cary, N.C. 27513, U.S.A.
 * ----------------------------------------------------------------------------
 *
 * NAME:        HiveHelper.java
 * DATE:        12 Oct 2011
 * SUPPORT:     sasdxs - Doug Sedlak
 *              joschl - Joe Schluter
 * AUTHOR:      Joe Schluter
 * PRODUCT:     SAS/ACCESS Engine for Hadoop
 * SCRIPT:      Vertical build.xml, ivy.xml
 * PURPOSE:     Hadoop Hive Java helper class.
 *
 * HISTORY:
 *   Defect    Action                               Date        Name
 *   ------------------------------------------------------------------------
 *             Initial implementation               09/12/2011  Joe Schluter
 *             Add hack for S0881096                07/18/2012  sasdxs
 *             Add 'deregister' method for cleanup  12/13/2012  dazant
 *             Add "fromSubject" method to connect  10/28/2015 dazant
 *---------------------------------------------------------------------------*/
//
// NOTE: The package name must match the class-path used in the call to tkJavaNewObject()
// called in hd_tkalen() in module sastkhdp.c!!!
package com.sas.access.hadoop.hive;
 

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/*From Subject Classes*/
import java.security.PrivilegedExceptionAction;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

import com.sun.security.auth.module.Krb5LoginModule;


//##################################################################################################
/*
    EXAMPLE debugging output to file:

    PrintStream f = new PrintStream("C:\\JavaDebug.txt");

    f.format("IOException: %s%n", e);
    f.format("getClass:    %s%n", e.getClass());
    f.format("getMessage:  %s%n", e.getMessage());

    f.close();
 */
//##################################################################################################


//##################################################################################################
//
//##################################################################################################
public class HiveHelper
{
    private static final Set<String> hive11TypeNames;
    static {
        Set<String> hive11TypeNames_1 = new HashSet<String>();
        hive11TypeNames_1.add("BOOLEAN");
        hive11TypeNames_1.add("TINYINT");
        hive11TypeNames_1.add("SMALLINT");
        hive11TypeNames_1.add("INT");
        hive11TypeNames_1.add("BIGINT");
        hive11TypeNames_1.add("FLOAT");
        hive11TypeNames_1.add("DOUBLE");
        hive11TypeNames_1.add("STRING");
        hive11TypeNames_1.add("TIMESTAMP");
        hive11TypeNames_1.add("BINARY");
        hive11TypeNames_1.add("DECIMAL");
        hive11TypeNames_1.add("ARRAY");
        hive11TypeNames_1.add("MAP");
        hive11TypeNames_1.add("STRUCT");
        hive11TypeNames_1.add("UNIONTYPE");
        hive11TypeNames_1.add("USER_DEFINED");
        hive11TypeNames = Collections.unmodifiableSet(hive11TypeNames_1);
        // hive12 adds DATE and VARCHAR
        // hive13 promises CHAR
        // do *not* update this set new types
        // this set is for HiveServer (not HiveServer2) since HivesServer
        // never implemented DatabaseMetaData.getTypeInfo()
    }


    private volatile int connectionFailureCode;
    private static final int CONNECT_OK = 0;
    private static final int CONNECT_FAIL_GENERAL = -1;
    private static final int CONNECT_FAIL_CNFE = -2;
    private static final int CONNECT_FAIL_TIMEOUT = -3;

    // S0937722:  Need to stop JDBC read thread (in fetchAll below).
    private volatile boolean mStop = false;

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // cancelReads
    //
    // S0937722:  Allow fetchAll to terminate gracefully in case like OBS= where SAS step runs down
    //            without reading entire result set.
    //
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    public void cancelReads()
    {
        mStop = true;
    }
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // renewReads
    //
    // S0937722:  Recommence normal processing
    //
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    public void renewReads()
    {
        mStop = false;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    // Nothing to do here (see comment below).
    //
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    public HiveHelper()
    {
        // Don't look for class availability (Class.forName()) here. We will decide which
        // specific HiveDriver to load in getConnection below.
    }


    /**************************************************************************************************
     * Deregister method
     * 		Deregister all drivers alloced currently.  This is needed such
     *	that the classloader can be garbage collected to prevent PermGen OutofMemory.
     **************************************************************************************************/
    public void deregister()
    {
        final Enumeration<Driver> drs = DriverManager.getDrivers();

        while(drs.hasMoreElements())
        {
            final Driver d = drs.nextElement();
            try {
                DriverManager.deregisterDriver(d);
            }
            catch (final SQLException e)
            {
                //Ignore; May mean that we can't garbage collect; but we tried.
            }
        }
    }


    // no properties or timeout
    public Connection getConnection(String uri, String user, String pass, String cls) throws Exception {
        return getConnection(uri, user, pass, null, cls, 0);
    }

    //no properties
    public Connection getConnection(String uri, String user, String pass, String cls, int timeout) throws Exception {
        return getConnection(uri, user, pass, null, cls, null, null, timeout);
    }    

    // no timeout
    public Connection getConnection(String uri, String user, String pass, String properties, String cls) throws Exception {
        return getConnection(uri, user, pass, properties, cls, 0);
    }

    public Connection getConnection(String uri,
            String user,
            String pass,
            String properties,
            String cls,
            int timeout) throws Exception
            {
        return getConnection(uri, user, pass, properties, cls, null, null, 0);
            }


    public static class LoginCallbackHandler implements CallbackHandler {
        final String _principal;
        final String _password;

        public LoginCallbackHandler(String principal, String password) {
            this._principal = principal;
            this._password = password;
        }

        public void handle(Callback[] callbacks)
                throws IOException, UnsupportedCallbackException {
            for (int i = 0; i < callbacks.length; i++) {
                if (callbacks[i] instanceof NameCallback) {
                    NameCallback nc = (NameCallback)callbacks[i];
                    nc.setName(this._principal);
                } else if (callbacks[i] instanceof PasswordCallback) {
                    PasswordCallback pc = (PasswordCallback)callbacks[i];
                    pc.setPassword(this._password.toCharArray());
                } else throw new UnsupportedCallbackException
                (callbacks[i], "Unrecognised callback");
            }
        }
    }

    static Subject getSubject(String jaasName, String userPrincipal, String password) throws LoginException
    {		
        LoginContext lc;

        if (jaasName == null || jaasName.length() <= 0)
        {
            // create a LoginContext based on the entry in the login.conf file
            lc = new LoginContext(jaasName, null, new LoginCallbackHandler(userPrincipal, password),
                    new javax.security.auth.login.Configuration(){
                public AppConfigurationEntry[] getAppConfigurationEntry(final String name){
                    final Map<String, ?> options=new HashMap();
                    AppConfigurationEntry a1=new AppConfigurationEntry(Krb5LoginModule.class.getName(),LoginModuleControlFlag.REQUIRED,options);			      
                    return new AppConfigurationEntry[]{a1};
                }});			      
        }
        else
        {
            lc = new LoginContext(jaasName, new LoginCallbackHandler(userPrincipal, password));
        }
        // login (effectively populating the Subject)
        lc.login();
        // get the Subject that represents the signed-on user
        return lc.getSubject();
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // getConnection
    //
    // NOTE: Due to an ill-behaved HiveDriver.connect() (see below), we must explicitly load the
    //       the driver from a given class (i.e. cannot use DriverManager.getConnection). This
    //       wrapper method handles this by allowing a user-specified class (cls parameter) or
    //       by using known default classes for hive and hive2.
    //
    // @param uri - String for uri
    // @param user - String for user name
    // @param pass - String for password
    // @param cls - String for class name (optional)
    // @param properties - Properties string, for example, "hive.security.authentication=password;hive.security.ssl=true"
    //                     With HiveServer2, the properties are appended to the URI. With HiveServer1, the properties are
    //                     passed along in a bag.
    // @param timeout - timeout in seconds, 0 gives you default of 30 seconds
    // @return Connection - created connection
    // @throws Exception -
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    public Connection getConnection(String uri,
            String user,
            String pass,
            String properties,
            String cls,
            String userpr,
            String jaasName,
            int timeout
            ) throws Exception
            {
        // always reset the failure code
        connectionFailureCode = CONNECT_OK;
        // With the advent of hive2, getting the correct HiveDriver has become a lot more difficult.
        // The two HiveDrivers (for hive and hive2) are ill-behaved in that they don't properly report
        // back to DriverManager.getConnection when it cannot process a given connection string (uri).
        // Instead it throws an exception. We therefore explicitly load and call the correct HiveDriver.

        // URI cannot be null
        if (uri == null) {
            return null;
        }

        // Select the correct HiveDriver class object
        final String className;
        if ((cls != null) && (cls.length() > 0))
            className = cls;
        else if (uri.substring(0, 10).equals("jdbc:hive:"))
            className = "org.apache.hadoop.hive.jdbc.HiveDriver";
        else  // "jdbc:hive2:" and anything else
            className = "org.apache.hive.jdbc.HiveDriver";

        // The connect() method requires a Properties object for user/pass
        Properties props = new Properties();                
        Subject userSubject = null;
        if (userpr != null && userpr.length() > 0)
        {
            userSubject = getSubject(jaasName, userpr, pass);
            props.put("auth","kerberos");
            props.put("kerberosAuthType","fromSubject");
        }
        else
        {
            if (user != null) {
                props.put("user", user);
            }
            if (pass != null) {
                props.put("password", pass);
            }
        }

        // key=value pattern
        Pattern pattern = Pattern.compile("([^;]*)=([^;]*)[;]?");

        // properties is not null only for HiveServer1...
        // for HiveServer2, the properties are appended to the URL
        if (properties != null) {
            Matcher matcher = pattern.matcher(properties);
            while (matcher.find()) {
                props.put(matcher.group(1), matcher.group(2));
            }
        }

        // Load the driver object
        try {
            Driver hiveDriver = (Driver)Class.forName(className).newInstance();
            // try to connect, but timeout 

            // user specified a timeout value in seconds
            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<Connection> future; 
            if (userpr == null || userpr.length() <= 0)
            {
                future = executor.submit(new ConnectionTimeoutCheck(hiveDriver, uri, props));
            }
            else
            {
                future = executor.submit(new ConnectionFromSubjectTimeoutCheck(hiveDriver, uri, props, userSubject));
            }

            try {
                // add timeout
                // default value is 30 seconds unless caller passed in non-zero value

                // amazingly, this can be null - not SQLException - with HiveService2 
                // jdbc:goober://hdp2ga.unx.sas.com
                return future.get(timeout == 0 ? 30 : timeout, TimeUnit.SECONDS);
            }
            catch (TimeoutException e) {
                connectionFailureCode = CONNECT_FAIL_TIMEOUT;
                throw e;
            }
            catch (InterruptedException e) {
                throw e;
            }
            catch (ExecutionException e) {
                // user would prefer to see the exception cause
                // cast is to avoid changing exception signature
                if (e.getCause() instanceof Exception) {
                    throw (Exception) e.getCause();
                } else {
                    throw e;
                }
            }
        } catch (ClassNotFoundException e) {
            connectionFailureCode = CONNECT_FAIL_CNFE;
            throw e;
        } catch (Exception e) {
            // set fail code if we didn't already do it
            if (connectionFailureCode == CONNECT_OK) {
                connectionFailureCode = CONNECT_FAIL_GENERAL;
            }
            throw e;
        }
            }

    /**
     * Returns a integer return code for the status of the last call to
     * getConnection().
     * 
     * The failure code is reset after each call to getConnection().
     */
    public int getConnectionFailureCode() {
        return connectionFailureCode;
    }

    static class ConnectionTimeoutCheck implements Callable<Connection> {

        final Driver hiveDriver;
        final String uri;
        final Properties properties;
        public ConnectionTimeoutCheck(Driver hiveDriver, String uri, Properties properties) {
            this.hiveDriver = hiveDriver;
            this.uri = uri;
            this.properties = properties;
        }

        @Override
        public Connection call() throws Exception
        {
            return hiveDriver.connect (uri, properties);
        }
    }


    static class ConnectionFromSubjectTimeoutCheck implements Callable<Connection> {

        final Driver _hiveDriver;
        final String _uri;
        final Properties _properties;
        final Subject _signedOnUserSubject; 
        public ConnectionFromSubjectTimeoutCheck(Driver hiveDriver, String uri, Properties properties, Subject userSubject) {
            this._hiveDriver = hiveDriver;
            this._uri = uri;
            this._properties = properties;
            this._signedOnUserSubject = userSubject;
        }

        @Override
        public Connection call() throws Exception
        {
            Connection conn = (Connection) Subject.doAs(this._signedOnUserSubject, new PrivilegedExceptionAction<Object>()
                    {
                public Object run() throws SQLException
                {    	        	  
                    Connection con = null;
                    try {
                        con = _hiveDriver.connect (_uri, _properties);
                    } catch (SQLException e) {
                        e.printStackTrace();
                        throw e;
                    } 
                    return con;
                }
                    });

            return conn;        	
        }
    }


    /*
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // tableExists (NO LONGER USED, but keep around as an example)
    //
    // @param connection - Connection to use to test for the table
    // @param tableName - String name of the table to test
    //
    // Returns TRUE if a table exists
    //
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    public boolean tableExists (Connection connection,
                                String     tableName) throws SQLException
    {
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("SHOW TABLES '" + tableName + "'");

        // If the ResultSet has at least one record, the table exists
        boolean exists = resultSet.next();
        statement.close();
        return exists;
    }
     */

    public static boolean supportsType(Connection conn, String typeName) throws SQLException {

        if (conn == null) {
            throw new NullPointerException("null parameter conn");
        }

        if (typeName == null) {
            throw new NullPointerException("null parameter typeName");
        }

        if (conn.isClosed()) {
            throw new SQLException("connection is closed");
        }

        // the right way to do this check is to call #getTypeInfo()
        // but HiveServer1 doesn't support #getTypeInfo()
        boolean hasTypeInfo = false;
        boolean supportsType = false;
        ResultSet rs = null;
        try {
            rs = conn.getMetaData().getTypeInfo();
            hasTypeInfo = true;
            while (rs.next()) {
                // with cdh451, TYPE_NAME is NULL for VARCHAR (java.sql.Types of 12)
                final String dbSupportedTypeName = rs.getString("TYPE_NAME") == null ? "" : rs.getString("TYPE_NAME").trim()
                        .toUpperCase();
                if (dbSupportedTypeName.equals(typeName)) {
                    supportsType = true;
                    break;
                }
            }

            if ("TIMESTAMP".equalsIgnoreCase(typeName) && supportsType) {
                // need minimum of Hive .12 for TIMESTAMP operations
                // see HIVE:2558, Timestamp comparisons don't work
                try {
                    // these calls are supported in HiveServer2, .12
                    // #getDatabaseMinorVersion/MajorVersion not supported in HiveServer .12
                    int driverMajorVersion = conn.getMetaData().getDriverMajorVersion();
                    int driverMinorVersion = conn.getMetaData().getDriverMinorVersion();
                    if (driverMajorVersion == 0 && driverMinorVersion < 12) {
                        supportsType = false;
                    }
                } catch (SQLException e) {
                    // earlier versions of HiveServer2 might not support metadata calls
                    supportsType = false;
                }
            }


        } catch (SQLException e) {
            // probably #getTypeInfo method is unsupported
        } finally {
            if (rs != null) {
                rs.close();
            }
        }

        // TODO: remove this code block when we no longer support HiveServer1
        // the wrong way (HiveServer1) do do this is to do a version check
        if (!hasTypeInfo) {
            // hiveServer1 implementation
            try {
                // these methods are not supported by HiveServer2, version .12
                // but are supported by HiveServer1
                int databaseMajorVersion = conn.getMetaData().getDatabaseMajorVersion();
                int databaseMinorVersion = conn.getMetaData().getDatabaseMinorVersion();
                // use the "known types", plus our version hacks
                supportsType = hive11TypeNames.contains(typeName);
                if (supportsType) {
                    if ("TIMESTAMP".equals(typeName)) {
                        // TIMESTAMP isn't usable until Hive JDBC 0.12
                        // see HIVE:2957, ResultSet#getColumnType broken
                        // see HIVE:2558, Timestamp comparisons don't work
                        if (databaseMajorVersion == 0 && databaseMinorVersion < 12) {
                            supportsType = false;
                        }
                    } else if ("DATE".equals(typeName)) {
                        // DATE since Hive 0.8
                        if (databaseMajorVersion == 0 && databaseMinorVersion < 8) {
                            supportsType = false;
                        }
                    } else if ("VARCHAR".equals(typeName)) {
                        // VARCHAR since Hive 0.12
                        if (databaseMajorVersion == 0 && databaseMinorVersion < 12) {
                            supportsType = false;
                        }
                    }
                }

            } catch (SQLException e) {
                // really? should never happen, but just do fast check
                supportsType = hive11TypeNames.contains(typeName);
            }
        } // hiveServer1 implementation

        return supportsType;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // openPipe
    //
    // Open the pipe, so it can report back SUCCESS before fetchAll is called
    //
    // @param pipeName - Name of the FileOutputStream to create
    // @return BufferedOutputStream - Opened pipe that data may be streamed through
    // @throws IOException
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    public BufferedOutputStream openPipe(String pipeName) throws IOException
    {
        // Try to open the pipe
        return new BufferedOutputStream(new FileOutputStream(pipeName), 8192);
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // closePipe
    //
    // Close the pipe. Called only there's an error calling fetchAll
    //
    // @param pipe - Pipe to close
    // @throws IOException
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    public void closePipe(BufferedOutputStream pipe) throws IOException
    {
        // Close the pipe
        pipe.close();
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // fetchAll
    //
    // Pump entire 'resultSet' through the pipe 'pipeName' in this format:
    //
    //     double:  eight byte IEEE double binary in little-endian format
    //     string:  four byte integer length-in-chars in little-endian format, followed by the string itself
    //     default (error): four bytes of 0xff
    //
    // @param resultSet - result set to read from
    // @param colTypes - String list of col types
    // @param fetchSize - size to fetch, or -1 for work-around fetch size
    // @param pipe - pipe to pump data to
    // @throws SQLException
    // @throws IOException
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    public void fetchAll(ResultSet             resultSet,
            String                colTypes,
            int                   fetchSize,
            BufferedOutputStream  pipe) throws SQLException, IOException
            {
        int    colCount = resultSet.getMetaData().getColumnCount();
        double dblVal;
        String strVal   = null;
        byte[] bytes = null;


        try  // Allow us to run cleanup code if an exception occurs ('finally' clause below)
        {
            // Set the number of records to fetch at once. This value is passed in to avoid jdbc
            // from allocating too much memory, in case each record is extremely long.

            try {
                resultSet.setFetchSize(fetchSize);
            } catch (SQLException e) {
                // eat exception if setFetchSize() isn't implemented,
                // as in early MapR - see HIVE-1815
            }

            // Loop over all records
            // S0937722:  Add !mStop.  Need to stop reading.
            while (!mStop && resultSet.next())
            {
                // Loop over all columns (1-based)
                for (int col = 1; col <= colCount; col++)
                {
                    switch (colTypes.charAt(col - 1))
                    {
                    case 'd':
                        // Write as double in little endian format
                        dblVal = resultSet.getDouble(col);
                        if (resultSet.wasNull())
                            pipe.write(toByteArray(-1, 8));  // MISSING data
                        else
                            pipe.write(toByteArray(Double.doubleToLongBits(dblVal), 8));
                        break;

                    case 's':
                        // Write string as 4-byte count (little endian) plus the data itself
                        strVal = resultSet.getString(col);
                        if (resultSet.wasNull())
                            pipe.write(toByteArray(-1, 4));  // MISSING data
                        else
                        {
                            bytes = strVal.getBytes("UTF-8");
                            pipe.write(toByteArray(bytes.length, 4));
                            pipe.write(bytes);
                        }
                        break;

                    case 'b':
                        // S0935698: support Hive Binary type (java.sql.Types.BINARY)
                        // Write string as 4-byte count (little endian) plus the data itself

                        if (resultSet.wasNull())
                            pipe.write(toByteArray(-1, 4));  // MISSING data
                        else
                        {
                            // no transcoding here
                            // would be better to call resultSet.getBytes()... stupid Hive
                            InputStream is = null;
                            try {
                                int length = 0;
                                is = resultSet.getBinaryStream(col);
                                ByteArrayOutputStream out = new ByteArrayOutputStream();
                                byte[] buff = new byte[4096]; // 4K buffer
                                while (true) {
                                    int buff_l = is.read(buff);
                                    if (buff_l == -1) {
                                        break;
                                    }
                                    out.write(buff, 0, buff_l);
                                    length += buff_l;
                                }

                                bytes = out.toByteArray();
                                pipe.write(toByteArray(length, 4));
                                pipe.write(bytes);
                            } catch (SQLException e) {
                                throw new SQLException("failed calling ResultSet.getBinaryStream(). "
                                        + "BINARY support requires Hive 0.13 or greater", e);
                            } finally {
                                if (is != null) {
                                    is.close();
                                }
                            }
                        }
                        break;
                    default:
                        // Send back an error indicator
                        pipe.write(toByteArray(-1, 8));  // Send back 8 bytes of 0xff
                        break;

                    }  //switch
                }  // for all columns

                // Write a 1-byte record separator to ensure integrity
                pipe.write(0xff);  // Send back 1 byte of 0xff

            }  // for all rows
        }
        catch (IOException e)
        {
            // Don't raise an exception if SAS just stopped reading
            if (!e.getMessage().equals("No process is on the other end of the pipe") &&  // Windows error message text
                    !e.getMessage().equals("Broken pipe"))                                   // Unix error message text
                throw e;
        }
        catch (SQLException e)
        {
            // Special case for defect S0885502
            if ("Error retrieving next row".equals(e.getMessage()))
                throw new RuntimeException("Error retrieving next row: " +
                        "This may be due to very long strings in the actual data " +
                        "and the use of DBSASTYPE to limit string length, " +
                        "causing a fetchSize too big to handle the actual data. " +
                        "Consider using the OS environment variable " +
                        "\"SAS_HADOOP_HIVE1815_WORKAROUND=YES\" " +
                        "to force the Hive fetchSize to 1. " +
                        "Contact Technical Support for details.", e);
            else
                throw e;
        }
        finally
        {
            // All done, close the pipe
            // Executes even if an exception occurred in the 'try' clause above
            pipe.close();
        }
            }


    ////////////////////////////////////////////////////////////////////////////////////////////////////
    //
    // toByteArray
    //
    // Convert an integer to an array of bytes in little-endian order
    //
    // @param - val - the value to convert
    // @param - bytes - length of bytes
    // @return byte array
    ////////////////////////////////////////////////////////////////////////////////////////////////////
    private byte[] toByteArray (long val,
            int bytes)
    {
        byte[] byteBuf = new byte[bytes];

        for (int b = 0; b < bytes; b++)
        {
            byteBuf[b] = (byte)(val & 0xff);
            val >>= 8;
        }
        return byteBuf;
    }

}  // End class