package gov.usgs.wma.waterdata.groundwater;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class BuildRdbFileTest {

	final String STATE  = "Wisconsin";
	final String POSTCD = "WI";
	final String POSTCD_BAD = "";
	final String FILENM = "mock-file-name";
	final String EMPTY_RDB_MSG = "Error writing RDB file to S3: empty RDB file created." +
			" [LocationFolder 'Wisconsin', States: [Wisconsin], S3file=" + FILENM + "]";
	final String RDB_INFO_MSG = "INFO: RDB file created, 6 rows " +
			" [LocationFolder 'Wisconsin', States: [Wisconsin], S3file=" + FILENM + "]";
	RdbWriter writer;
	OutputStream out;
	Writer destination;
	RequestObject req;
	List<String> stateAsList;

	boolean outStreamClosed;
	boolean dstWriterClosed;

	@BeforeEach
	public void beforeEach() {
		req = new RequestObject();
		req.locationFolder = STATE;
		stateAsList = List.of(STATE);
		outStreamClosed = false;

		out = new ByteArrayOutputStream() {
			@Override
			public void close() throws IOException {
				outStreamClosed = true;
				super.close();
			}
		};

		dstWriterClosed = false;

		destination = new OutputStreamWriter(out) {
			@Override
			public void close() throws IOException {
				dstWriterClosed = true;
				super.close();
			}
		};

		writer = new RdbWriter(destination) {
			@Override
			public long getDataRowCount() {
				return 6;
			}
		};
	}

	@Test
	void testHappyPath() throws Exception {
		// SETUP
		S3Bucket mockS3b = Mockito.mock(S3Bucket.class);
		mockS3b.writer = destination;
		Mockito.when(mockS3b.getWriter()).thenReturn(destination);
		Mockito.doCallRealMethod().when(mockS3b).close();
		Mockito.when(mockS3b.sendS3()).thenReturn(null);
		Mockito.when(mockS3b.getKeyName()).thenReturn(FILENM);

		// In the actual code it has the .gz extension.
		// This test does not require the extension for filename testing.

		S3BucketUtil mockS3u = Mockito.mock(S3BucketUtil.class);
		Mockito.when(mockS3u.createFilename(POSTCD)).thenReturn(FILENM);
		Mockito.when(mockS3u.openS3(FILENM)).thenReturn(mockS3b);

		SnsUtil mockSns = Mockito.mock(SnsUtil.class);

		DiscreteGroundWaterDao mockDao = Mockito.mock(DiscreteGroundWaterDao.class);

		AqToNwisParmDao mockAqDao = Mockito.mock(AqToNwisParmDao.class);
		LocationFolder mockLoc = Mockito.mock(LocationFolder.class);
		Mockito.when(mockLoc.toStates(STATE)).thenReturn(stateAsList);
		Mockito.when(mockLoc.filenameDecorator(STATE)).thenReturn(POSTCD);
		Mockito.when(mockAqDao.getParameters()).thenReturn(getParameterList());

		BuildRdbFile builder = new BuildRdbFile() {
			@Override
			protected RdbWriter createRdbWriter(Writer destination) {
				if (destination != writer.rdb) {
					throw new RuntimeException("For the unit test to be correctly setup the writers must be the same.");
				}
				return writer;
			}
		};
		builder.dao = mockDao;
		builder.aqDao = mockAqDao;
		builder.s3BucketUtil = mockS3u;
		builder.snsUtil = mockSns;
		builder.locationFolderUtil = mockLoc;

		// ACTION UNDER TEST
		ResultObject res = builder.apply(req);

		// ASSERTIONS
		assertEquals(4, writer.getHeaderRowCount(), "The header should be written in the RDB file builder.");
		assertEquals(6, res.getCount(), "The result object should contain the number of rows written.");
		assertTrue(res.getMessage().contains(FILENM), "The result object should contain the filename placed in S3.");
		Mockito.verify(mockDao, Mockito.atLeastOnce()).sendDiscreteGroundWater(stateAsList, writer, mockAqDao.getParameters());
		Mockito.verify(mockS3b, Mockito.atLeastOnce()).getWriter();
		Mockito.verify(mockS3b, Mockito.atMostOnce()).getWriter();
		Mockito.verify(mockS3b, Mockito.atLeastOnce()).close();
		Mockito.verify(mockS3u, Mockito.atLeastOnce()).createFilename(POSTCD);
		Mockito.verify(mockS3u, Mockito.atLeastOnce()).openS3(FILENM);
		Mockito.verify(mockSns, Mockito.atLeastOnce()).publishSNSMessage(RDB_INFO_MSG);
		Mockito.verify(mockSns, Mockito.atMostOnce()).publishSNSMessage(RDB_INFO_MSG);
		Mockito.verify(mockLoc, Mockito.atLeastOnce()).toStates(STATE);
		Mockito.verify(mockLoc, Mockito.atLeastOnce()).filenameDecorator(STATE);
		assertTrue(dstWriterClosed);
		assertTrue(outStreamClosed);
	}

	@Test
	void testBadLocationFolder() throws Exception {
		// SETUP

		S3Bucket mockS3b = Mockito.mock(S3Bucket.class);
		Mockito.when(mockS3b.getWriter()).thenReturn(destination);

		S3BucketUtil mockS3u = Mockito.mock(S3BucketUtil.class);
		Mockito.when(mockS3u.createFilename(POSTCD)).thenReturn(FILENM);
		Mockito.when(mockS3u.openS3(FILENM)).thenReturn(mockS3b);

		DiscreteGroundWaterDao mockDao = Mockito.mock(DiscreteGroundWaterDao.class);

		LocationFolder mockLoc = Mockito.mock(LocationFolder.class);
		Mockito.when(mockLoc.toStates(STATE)).thenReturn(stateAsList);
		Mockito.when(mockLoc.filenameDecorator(STATE)).thenReturn(POSTCD_BAD);

		BuildRdbFile builder = new BuildRdbFile() {
			@Override
			protected RdbWriter createRdbWriter(Writer destination) {
				if (destination != writer.rdb) {
					throw new RuntimeException("For the unit test to be correctly setup the writers must be the same.");
				}
				return writer;
			}
		};
		builder.dao = mockDao;
		builder.s3BucketUtil = mockS3u;
		builder.locationFolderUtil = mockLoc;

		// ACTION UNDER TEST
		assertThrows(RuntimeException.class, ()->builder.apply(req) );

		// ASSERTIONS
		assertEquals(0, writer.getHeaderRowCount(), "The header should NOT be written in the RDB file builder for bad location folder.");
		Mockito.verify(mockDao, Mockito.never()).sendDiscreteGroundWater(stateAsList, writer, getParameterList());
		Mockito.verify(mockS3b, Mockito.never()).getWriter();
		Mockito.verify(mockS3b, Mockito.never()).close();
		Mockito.verify(mockS3u, Mockito.never()).createFilename(POSTCD);
		Mockito.verify(mockS3u, Mockito.never()).openS3(FILENM);
		Mockito.verify(mockLoc, Mockito.atLeastOnce()).toStates(STATE);
		Mockito.verify(mockLoc, Mockito.atLeastOnce()).filenameDecorator(STATE);
		assertFalse(outStreamClosed);
		assertFalse(dstWriterClosed);
	}

	@Test
	void testIOException() throws Exception {
		// SETUP

		S3Bucket mockS3b = Mockito.mock(S3Bucket.class);
		mockS3b.writer = destination;
		Mockito.when(mockS3b.getWriter()).thenReturn(destination);
		Mockito.doCallRealMethod().when(mockS3b).close();

		S3BucketUtil mockS3u = Mockito.mock(S3BucketUtil.class);
		Mockito.when(mockS3u.createFilename(POSTCD)).thenReturn(FILENM);
		Mockito.when(mockS3u.openS3(FILENM)).thenReturn(mockS3b);

		DiscreteGroundWaterDao mockDao = Mockito.mock(DiscreteGroundWaterDao.class);

		LocationFolder mockLoc = Mockito.mock(LocationFolder.class);
		Mockito.when(mockLoc.getLocationFolders()).thenReturn(stateAsList);
		Mockito.when(mockLoc.filenameDecorator(STATE)).thenReturn(POSTCD);

		BuildRdbFile builder = new BuildRdbFile() {
			@Override
			protected RdbWriter createRdbWriter(Writer destination) {
				writer = Mockito.mock(RdbWriter.class);
				Mockito.when(writer.writeHeader()).thenThrow(new IOException("Unit test IOE"));
				return writer;
			}
		};
		builder.dao = mockDao;
		builder.s3BucketUtil = mockS3u;
		builder.locationFolderUtil = mockLoc;

		// ACTION UNDER TEST
		assertThrows(RuntimeException.class, ()->builder.apply(req), "IOE converted to Runtime");

		// ASSERTIONS
		assertEquals(0, writer.getHeaderRowCount(), "The header should NOT be written in the RDB file builder for bad location folder.");
		Mockito.verify(mockLoc, Mockito.atLeastOnce()).toStates(STATE);
		Mockito.verify(mockLoc, Mockito.atLeastOnce()).filenameDecorator(STATE);
		Mockito.verify(mockS3b, Mockito.times(1)).getWriter();
		Mockito.verify(mockS3b, Mockito.atLeastOnce()).close();
		Mockito.verify(mockS3u, Mockito.atLeastOnce()).createFilename(POSTCD);
		Mockito.verify(mockS3u, Mockito.atLeastOnce()).openS3(FILENM);
		Mockito.verify(mockDao, Mockito.never()).sendDiscreteGroundWater(stateAsList, writer, getParameterList());
		assertTrue(outStreamClosed);
		assertTrue(dstWriterClosed);
	}

	@Test
	void testEmptyRdbException() throws Exception {
		// SETUP

		S3Bucket mockS3b = Mockito.mock(S3Bucket.class);
		mockS3b.writer = destination;
		Mockito.when(mockS3b.getWriter()).thenReturn(destination);
		Mockito.doCallRealMethod().when(mockS3b).close();

		S3BucketUtil mockS3u = Mockito.mock(S3BucketUtil.class);
		Mockito.when(mockS3u.createFilename(POSTCD)).thenReturn(FILENM);
		Mockito.when(mockS3u.openS3(FILENM)).thenReturn(mockS3b);

		SnsUtil mockSns = Mockito.mock(SnsUtil.class);

		DiscreteGroundWaterDao mockDao = Mockito.mock(DiscreteGroundWaterDao.class);
		AqToNwisParmDao mockAqDao = Mockito.mock(AqToNwisParmDao.class);

		LocationFolder mockLoc = Mockito.mock(LocationFolder.class);
		Mockito.when(mockLoc.getLocationFolders()).thenReturn(stateAsList);
		Mockito.when(mockLoc.toStates(STATE)).thenReturn(stateAsList);
		Mockito.when(mockLoc.filenameDecorator(STATE)).thenReturn(POSTCD);
		Mockito.when(mockAqDao.getParameters()).thenReturn(getParameterList());

		BuildRdbFile builder = new BuildRdbFile() {
			@Override
			protected RdbWriter createRdbWriter(Writer destination) {
				writer = Mockito.mock(RdbWriter.class);
				Mockito.when(writer.writeHeader()).thenReturn(writer);
				Mockito.when(writer.getDataRowCount()).thenReturn(0L);
				return writer;
			}
		};
		builder.aqDao = mockAqDao;
		builder.dao = mockDao;
		builder.s3BucketUtil = mockS3u;
		builder.snsUtil = mockSns;
		builder.locationFolderUtil = mockLoc;

		// ACTION UNDER TEST
		RuntimeException e = assertThrows(RuntimeException.class, ()->builder.apply(req), "Expected RuntimeException to have been thrown");
		assertEquals(EMPTY_RDB_MSG, e.getMessage());

		// ASSERTIONS
		Mockito.verify(mockAqDao, Mockito.times(1)).getParameters();
		Mockito.verify(mockLoc, Mockito.times(1)).toStates(STATE);
		Mockito.verify(mockLoc, Mockito.times(1)).filenameDecorator(STATE);
		Mockito.verify(mockS3b, Mockito.times(1)).getWriter();
		Mockito.verify(mockS3b, Mockito.times(1)).close();
		Mockito.verify(mockS3b, Mockito.never()).sendS3();
		Mockito.verify(mockS3u, Mockito.times(1)).createFilename(POSTCD);
		Mockito.verify(mockS3u, Mockito.times(1)).openS3(FILENM);
		Mockito.verify(mockSns, Mockito.times(1)).publishSNSMessage(EMPTY_RDB_MSG);
		Mockito.verify(mockDao, Mockito.times(1)).sendDiscreteGroundWater(stateAsList, writer, mockAqDao.getParameters());
		assertTrue(outStreamClosed);
		assertTrue(dstWriterClosed);
	}

	@Test
	void testAllLocationFolder() {
		// SETUP
		S3BucketUtil mockS3u = Mockito.mock(S3BucketUtil.class);
		DiscreteGroundWaterDao mockDao = Mockito.mock(DiscreteGroundWaterDao.class);
		LocationFolder mockLoc = Mockito.mock(LocationFolder.class);

		BuildRdbFile builder = new BuildRdbFile() {
			@Override
			protected ResultObject processAllRequest(Collection<String> locationFolders) {
				ResultObject result = new ResultObject();
				result.setCount(-1);
				result.setMessage("TESTING");
				return result;
			}
		};
		builder.dao = mockDao;
		builder.s3BucketUtil = mockS3u;
		builder.locationFolderUtil = mockLoc;

		req.locationFolder = "ALL";

		// ACTION UNDER TEST
		ResultObject res = builder.apply(req);

		// ASSERTIONS
		assertEquals(-1, res.getCount());
		assertEquals("TESTING", res.getMessage());
		assertEquals(0, writer.getHeaderRowCount());
		Mockito.verify(mockLoc, Mockito.atLeastOnce()).getLocationFolders();
		Mockito.verify(mockLoc, Mockito.atMostOnce()).getLocationFolders();
		Mockito.verify(mockLoc, Mockito.never()).toStates(STATE);
		Mockito.verify(mockLoc, Mockito.never()).filenameDecorator(STATE);
		Mockito.verify(mockS3u, Mockito.never()).createFilename(POSTCD);
		Mockito.verify(mockS3u, Mockito.never()).openS3(FILENM);
		Mockito.verify(mockDao, Mockito.never()).sendDiscreteGroundWater(stateAsList, writer, getParameterList());
		assertFalse(outStreamClosed);
		assertFalse(dstWriterClosed);
	}

	private List<Parameter> getParameterList() {
		List<Parameter> parameters = new ArrayList<>();
		Parameter p1 = new Parameter();
		p1.setParameterCode("30210");
		p1.setBelowLandSurface(true);
		p1.setAboveDatum(false);
		parameters.add(p1);
		return parameters;

	}
}
