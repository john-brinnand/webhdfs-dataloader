package datavalidator.webhdfs.exception;

public class WebHdfsException extends RuntimeException {

	private static final long serialVersionUID = 1L;
	
	public WebHdfsException() {
		super();
	}
	
	public WebHdfsException (String message) {
		super(message);
	}
	
	public WebHdfsException(Throwable cause) {
		super(cause);
	}
	
	public WebHdfsException (String message, Throwable cause) {
		super(message, cause);
	}	

}
