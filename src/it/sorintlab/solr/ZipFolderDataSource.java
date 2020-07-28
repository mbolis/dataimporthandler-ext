package it.sorintlab.solr;

import static org.apache.solr.handler.dataimport.DataImportHandlerException.SEVERE;
import static org.apache.solr.handler.dataimport.DataImportHandlerException.wrapAndThrow;

import java.io.IOException;
import java.io.Reader;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;

import org.apache.solr.handler.dataimport.Context;
import org.apache.solr.handler.dataimport.DataImportHandlerException;
import org.apache.solr.handler.dataimport.DataSource;
import org.apache.solr.handler.dataimport.URLDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZipFolderDataSource extends DataSource<Reader> {
  public static final String BASE_PATH = "basePath";
  
  /**
   * The basePath for this data source
   */
  protected String basePath;
  
  /**
   * The encoding using which the given file should be read
   */
  protected String encoding = null;
  
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  
  @Override
  public void init(Context context, Properties initProps) {
    basePath = initProps.getProperty(BASE_PATH);
    if (initProps.get(URLDataSource.ENCODING) != null) {
      encoding = initProps.getProperty(URLDataSource.ENCODING);
    }
  }
  
  @Override
  public Reader getData(final String query) {
    log.info("Opening reader for query: {}", query);
    final Path filePath = getPath(basePath, query);
    if (Files.isRegularFile(filePath)) {
      return openFileReader(filePath);
    }
    return openZipFileReader(filePath);
  }
  
  private Reader openFileReader(final Path filePath) {
    try {
      final Reader reader = openStream(filePath);
      log.info("File '{}' found", filePath);
      return reader;
    } catch (IOException e) {
      wrapAndThrow(SEVERE, e, "Unable to open File : " + filePath);
      return null;
    }
  }
  
  private Reader openZipFileReader(final Path filePath) {
    final Path dirPath = filePath.getParent();
    log.info("Looking for ZIP files in parent path: {}", dirPath);
    try {
      final Path zipPath = Files.list(dirPath).filter(file -> file.toString().endsWith(".zip")).findFirst()
          .orElseThrow(() -> new DataImportHandlerException(SEVERE, "No ZIP file found in Directory : " + dirPath));
      
      final String innerPath = filePath.getFileName().toString();
      final Path innerFilePath = FileSystems.newFileSystem(zipPath, null).getPath(innerPath);
      try {
        final Reader reader = openStream(innerFilePath);
        log.info("File '{}' found inside ZIP '{}'", innerPath, zipPath);
        return reader;
      } catch (IOException e) {
        wrapAndThrow(SEVERE, e, "Unable to locate File : " + innerPath + " inside ZIP File : " + zipPath);
        return null;
      }
      
    } catch (IOException e) {
      wrapAndThrow(SEVERE, e, "Unable to open Directory : " + dirPath);
      return null;
    }
  }
  
  private Path getPath(final String basePath, final String query) {
    final Path root = Optional.ofNullable(basePath).map(base -> {
      final Path path = Paths.get(base);
      if (!path.isAbsolute()) {
        final Path absolutePath = path.toAbsolutePath();
        log.warn("ZipFolderDataSource.basePath is not absolute. Resolving to: {}", absolutePath);
        return absolutePath;
      }
      return path;
    }).orElseGet(() -> {
      final Path path = Paths.get(".").toAbsolutePath();
      log.warn("ZipFolderDataSource.basePath is empty. Resolving to: {}", path);
      return path;
    });
    
    final Path path = root.resolve(query);
    log.info("Accessing file: {}", path);
    return path;
  }
  
  protected Reader openStream(final Path file) throws IOException {
    final Charset charset = Optional.ofNullable(encoding).map(Charset::forName).orElseGet(Charset::defaultCharset);
    return Files.newBufferedReader(file, charset);
  }
  
  @Override
  public void close() {
    
  }
  
}
