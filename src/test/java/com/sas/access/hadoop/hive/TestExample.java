package com.sas.access.hadoop.hive;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import org.junit.Test;
import org.mockito.Mockito;

import junit.framework.TestCase;

public class TestExample extends TestCase {

	@Test
	public void testAssertTrue() {
		assertTrue(true);
	}

	public void testFetchAll() throws Exception {

		// mock up result set meta data - 2 columns
		ResultSetMetaData rsmdMock = Mockito.mock(ResultSetMetaData.class);
		Mockito.when(rsmdMock.getColumnCount()).thenReturn(2);

		// mock up one row result set, first column is 1.0d, 2nd is "test1"
		ResultSet rsMock = Mockito.mock(ResultSet.class);
		Mockito.when(rsMock.getDouble(1)).thenReturn(1.0d);
		Mockito.when(rsMock.getString(2)).thenReturn("test1");
		Mockito.when(rsMock.next()).thenReturn(true).thenReturn(false);
		Mockito.doReturn(rsmdMock).when(rsMock).getMetaData();

		// pull the result set through HiveHelper. Use a byte stream, so that
		// we can inspect the bytes written to the "pipe" by HiveHelper
		HiveHelper hh = new HiveHelper();
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		BufferedOutputStream bs = new BufferedOutputStream(byteStream);
		hh.fetchAll(rsMock, "ds" /* column types 'd' double, 's' string */,
				10 /* fetch size */, bs);

		byte[] bytes = byteStream.toByteArray();
		ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);

		// getDouble(1) has value of 1.0d
		assertEquals(1.0d, bb.getDouble(), 0.001);

		// length of string is 5 chars
		assertEquals(5, bb.getInt());

		// space for the new string
		byte[] b2 = new byte[5];
		bb.get(b2, 0, 5);
		assertEquals("test1", new String(b2, "UTF-8"));

		// // 1 byte of 0xff
		byte b4 = bb.get();
		assertEquals(-1, b4);


	}

}
