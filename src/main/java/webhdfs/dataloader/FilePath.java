package webhdfs.dataloader;

import java.io.File;

import lombok.Getter;

@Getter
public class FilePath {
	private File file;
	
	private FilePath (Builder builder) {
		this.file = builder.file;
	}

	public static class Builder {
		private File  file;
		
		public Builder () {
			file =  new File(File.separator);
		}

		public Builder addPathSegment(String pathSegment) {
			if (file == null || file.getPath().equals(File.separator)) {
				file = new File(file, pathSegment);
				
			}
			else {
				file = new File(file, File.separator + pathSegment);
			}
			return this;
		}
		public FilePath build() {
			return new FilePath(this);
		}
	}
}
