package webhdfs.dataloader;

public enum WebHdfsOps {
	CREATE("CREATE"),
	APPEND("APPEND"),
	LISTSTATUS("LISTSTATUS"),
	GETFILESTATUS ("GETFILESTATUS"),
	GETCONTENTSUMMARY("GETCONTENTSUMMARY"),
	MKDIRS("MKDIRS"),
	SETOWNER("SETOWNER");
	
	private final String value;

	private WebHdfsOps(String value) {
		this.value = value;
	}

	/**
	 * Return the integer value of this status code.
	 */
	public String value() {
		return this.value;
	}
}
