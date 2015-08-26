package webhdfs.dataloader;

/**
 * @author jbrinnand
 */
public class WebHdfsParams {
	public final static String CREATE = "CREATE";
	public final static String APPEND = "APPEND";
	public final static String LISTSTATUS = "LISTSTATUS";
	public final static String GETFILESTATUS = "GETFILESTATUS";
	public final static String GETCONTENTSUMMARY = "GETCONTENTSUMMARY";
	public final static String MKDIRS = "MKDIRS";
	public final static String SETOWNER = "SETOWNER";
	public final static String OWNER = "owner";
	public final static String GROUP = "group";
	public final static String OP = "op";
	public final static String USER = "user";
	public final static String USERNAME = "user.name";
	public final static String OVERWRITE = "overwrite";
	public final static String LOCATION = "location";
	public final static String PERMISSION = "permission";
	public final static String PERMISSIONS = "permissions";
	public final static String DEFAULT_PERMISSIONS = "755";
	public final static String TYPE = "type";
	public final static String FILE = "FILE";
	public final static String FILE_STATUSES = "FileStatuses";
	public final static String FILE_STATUS = "FileStatus";
	public final static String CONTENT_SUMMARY = "ContentSummary";
	public final static String DIRECTORY_COUNT = "directoryCount";
	public final static String REDIRECT_URI_LOCATION = LOCATION + ":" + "0";
	
}
