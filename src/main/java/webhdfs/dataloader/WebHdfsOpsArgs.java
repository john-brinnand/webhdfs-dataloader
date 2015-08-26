package webhdfs.dataloader;

import org.springframework.http.HttpStatus;

import lombok.Getter;
import lombok.Setter;

/**
 * @author jbrinnand
 */
@Getter @Setter
public class WebHdfsOpsArgs {
	private WebHdfsOps webHdfsOp;
	private Object[] args;
	private HttpStatus httpStatus;
	
	public WebHdfsOpsArgs(WebHdfsOps op, Object...args) {
		this.webHdfsOp = op;
		this.args = args;
	}
	
	public WebHdfsOpsArgs(WebHdfsOps op, HttpStatus httpStatus, Object...args) {
		this.webHdfsOp = op;
		this.httpStatus = httpStatus;
		this.args = args;
	}	
}
