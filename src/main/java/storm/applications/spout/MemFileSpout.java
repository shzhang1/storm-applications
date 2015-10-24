package storm.applications.spout;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.spout.parser.Parser;
import storm.applications.util.config.ClassLoaderUtils;
import storm.applications.util.io.FileUtils;
import storm.applications.util.stream.StreamValues;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.*;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class MemFileSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(MemFileSpout.class);

    protected Parser parser;
    protected File files;
    protected List<String> str_l;

    protected int curFileIndex = 0;
    protected int curLineIndex = 0;
    private boolean finished = false;

    protected int taskId;
    protected int numTasks;
    protected int index_e = 0;
    protected int element = 0;

    @Override
    public void initialize() {
        taskId = context.getThisTaskIndex();//context.getThisTaskId();
        numTasks = config.getInt(getConfigKey(BaseConf.SPOUT_THREADS));
        String path = config.getString(getConfigKey(BaseConf.SPOUT_PATH));
        String parserClass = config.getString(getConfigKey(BaseConf.SPOUT_PARSER));
        parser = (Parser) ClassLoaderUtils.newInstance(parserClass, "parser", LOG);
        parser.initialize(config);
        str_l = new LinkedList<String>();
        //  buildIndex();
        try {
            openFile(path, str_l);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void openFile(String fileName, List<String> str_l) throws FileNotFoundException {
        Scanner scanner = new Scanner(new File(fileName));
        while (scanner.hasNext()) {
            str_l.add(scanner.nextLine());
        }


    }


    @Override
    public void nextTuple() {
        String value = null;
        if (index_e < str_l.size())
            value = str_l.get(index_e++);

        if (value == null)
            return;

        List<StreamValues> tuples = parser.parse(value);

        if (tuples != null) {
            for (StreamValues values : tuples) {
                String msgId = String.format("%d%d", curFileIndex, curLineIndex);
                collector.emit(values.getStreamId(), values, msgId);
            }
        }
    }


}	
