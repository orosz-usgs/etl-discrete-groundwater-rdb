package gov.usgs.wma.waterdata.groundwater;

import java.io.Writer;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * AWS Entry point and orchestration of GW RDB file export.
 *
 * @author duselman
 */
@Component
public class BuildRdbFile implements Function<RequestObject, ResultObject> {

	private static final Logger LOG = LoggerFactory.getLogger(BuildRdbFile.class);

	@Autowired
	protected S3BucketUtil s3BucketUtil;

	@Autowired
	protected SnsUtil snsUtil;

	@Autowired
	protected AqToNwisParmDao aqDao;

	@Autowired
	protected DiscreteGroundWaterDao dao;

	@Autowired
	protected LocationFolder locationFolderUtil;

	@Autowired
	protected Properties properties;


	public BuildRdbFile() {
	}

	/**
	 * Entry point of AWS lambda processing.
	 *
	 * @param request an AQTS location folder
	 * @return TBD (writes file to S3 Bucket)
	 */
	@Override
	public  ResultObject apply(RequestObject request) {
		LOG.debug("the request object location folder: {}", request);
		String locationFolder = request.getLocationFolder();

		if ("ALL".equals(locationFolder)) {
			return processAllRequest(locationFolderUtil.getLocationFolders());
		}
		return processRequest(locationFolder);
	}

	protected ResultObject processAllRequest(Collection<String> locationFolders) {
		return new InvokeAll().invoke(properties, locationFolders);
	}

	/**
	 * Orchestration of AWS lambda processing.
	 * Fleshes out the location folder into a list of states.
	 * Translates the location folder into a file decorator.
	 * Fetches the GW Data and writes it to an S3 Bucket file.
	 *
	 * @param locationFolder an AQTS location folder
	 * @return result number of rows written to RDB file
	 */
	protected ResultObject processRequest(String locationFolder) {
		LOG.debug("the request location folder: {}", locationFolder);
		ResultObject result = new ResultObject();

		List<String> states = locationFolderUtil.toStates(locationFolder);
		String mess = "";

		String suffix = locationFolderUtil.filenameDecorator(locationFolder);
		if (!StringUtils.hasText(suffix)) {
			mess = "Given location folder has no state entry: " + locationFolder;
			snsUtil.publishSNSMessage("ERROR: " + mess);
			throw new RuntimeException(mess);
		}
		String filename = s3BucketUtil.createFilename(suffix);
		String details = String.format(" [LocationFolder '%s', States: %s, S3file=%s]", locationFolder,
			states.toString(), filename);

		try (S3Bucket s3bucket = s3BucketUtil.openS3(filename)) {

			Writer writer = s3bucket.getWriter();
			RdbWriter rdbWriter = createRdbWriter(writer).writeHeader();
			dao.sendDiscreteGroundWater(states, rdbWriter, aqDao.getParameters());

			// Currently, an empty rdb file is a business rule violation as it
			// would delete all the sites in NWISWEB for a given location (IOW-728).
			if (rdbWriter.getDataRowCount() == 0) {
				mess = "empty RDB file created.";
				snsUtil.publishSNSMessage("ERROR: " + mess + details);
				throw new RuntimeException(mess);
			}

			s3bucket.sendS3();

			result.setCount( (int)rdbWriter.getDataRowCount() );
			result.setMessage("Count is rows written to file: " + s3bucket.getKeyName());
			mess = String.format("INFO: RDB file created, %d rows %s",
					rdbWriter.getDataRowCount(), details);
			snsUtil.publishSNSMessage(mess);
		} catch (Exception e) {
			mess = "Error writing RDB file to S3: " + e.getMessage() + details;
			snsUtil.publishSNSMessage(mess);
			throw new RuntimeException(mess, e);
		}

		// currently returning the rows count written to the file
		return result;
	}

	/**
	 * Helper method that makes test injection easier.
	 * @param destination destination writer
	 * @return RDB wrapper writer instance
	 */
	protected RdbWriter createRdbWriter(Writer destination) {
		return new RdbWriter(destination);
	}
}
