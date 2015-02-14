package de.nick.solr.feeder;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;

public class FeederTest {

    private Logger log = org.slf4j.LoggerFactory.getLogger(FeederTest.class);
    private String url = "http://localhost:8080/solr";
    private SolrServer server;
    private SimpleDateFormat dateFormatUTC = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

    @Before
    public void setup() {
        server = new HttpSolrServer(url);
    }

    @Test
    public void test() throws Exception {
        server.deleteByQuery("*:*");

        final Path path = Paths.get("C://Users/nwi/");
        log.info("List files for path {}", path.toAbsolutePath());
        final List<Path> pathes = listFiles(path).collect(toList());
        log.info("Found {} documents", pathes.size());

        log.info("Adding documents to solr");
        pathes.parallelStream().filter(p -> p.toFile().isFile()).forEach((path1) -> feedPath(path1));

        log.info("commit");
        server.commit();
        log.info("{} documents indexed", pathes.size());
    }

    private void feedPath(Path path) {
        try {
            final File file = path.toFile();

            final String absolutePath = file.getAbsolutePath();
            log.info("Adding document {}", absolutePath);

            final SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", file.hashCode());
            doc.addField("name", file.getName());

            final String filePath = absolutePath.replace("\\", "/");
            doc.addField("path", filePath);

            final List<String> splitPath = Lists.newArrayList(Splitter.on("/").omitEmptyStrings().split(filePath));
            int i = 1;
            for (String pathX : splitPath) {
                if (i > 5) break;
                doc.addField("path_" + i, pathX);
                i++;
            }

            Date lastModified = new Date(file.lastModified());
            doc.addField("last_modified", dateFormatUTC.format(lastModified));
            doc.addField("fileSize", file.length() / 1024);

            server.add(doc);
            //server.commit();
        } catch (IOException | SolrServerException e) {
            throw new RuntimeException(e);
        }
    }

    public static Stream<Path> listFiles(Path path) {
        if (Files.isDirectory(path)) {
            try {
                return Files.list(path).parallel().flatMap(FeederTest::listFiles);
            } catch (Exception e) {
                return Stream.empty();
            }
        } else {
            return Stream.of(path);
        }
    }

}
