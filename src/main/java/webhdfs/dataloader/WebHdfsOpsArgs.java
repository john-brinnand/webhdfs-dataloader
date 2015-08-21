package webhdfs.dataloader;

import lombok.Getter;
import lombok.Setter;

/**
 * @author jbrinnand
 */
@Getter @Setter
public class WebHdfsOpsArgs {
	private WebHdfsOps webHdfsOp;
	private Object[] args;
	
	public WebHdfsOpsArgs(WebHdfsOps op, Object...args) {
		this.webHdfsOp = op;
		this.args = args;
	}
}
