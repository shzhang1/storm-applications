package storm.applications.spout;

import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import org.apache.thrift7.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.applications.constants.BaseConstants.BaseConf;
import storm.applications.spout.generator.Generator;
import storm.applications.util.config.ClassLoaderUtils;
import storm.applications.util.config.Configuration;
import storm.applications.util.stream.StreamValues;
import java.util.List;
import java.util.Map;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class GeneratorSpout extends AbstractSpout {
    private static final Logger LOG = LoggerFactory.getLogger(GeneratorSpout.class);
    private Generator generator;
    private long count=100000000;
    StreamValues values=null;
    @Override
    protected void initialize() {
        String generatorClass = config.getString(getConfigKey(BaseConf.SPOUT_GENERATOR));
        generator = (Generator) ClassLoaderUtils.newInstance(generatorClass, "parser", LOG);
        generator.initialize(config);
        
    	Configuration Conf = Configuration.fromMap(config);
    	count=Conf.getInt("count", 1000000);        
        
    }

    @Override
    public void nextTuple() {
    	if(count-->0){
        values = generator.generate();
    	}else{
    		values=null;
    	}
        if (values == null) {
        	System.out.println("Finished, try to kill");
            Map conf = Utils.readStormConfig();
            Nimbus.Client client = NimbusClient.getConfiguredClient(conf).getClient();
            try {
                List<TopologySummary> topologyList = client.getClusterInfo().get_topologies();
              while(topologyList.size()==0)
                  topologyList = client.getClusterInfo().get_topologies();

               // KillOptions killOpts = new KillOptions();
                    //killOpts.set_wait_secs(waitSeconds) // time to wait before killing
                client.killTopology(topologyList.get(0).get_name()); //provide topology name

            } catch (TException e) {
                e.printStackTrace();
            } catch (NotAliveException e) {
                e.printStackTrace();
            }
        }
        if(values!=null) {
            collector.emit(values.getStreamId(), values);
        }
    }
}
