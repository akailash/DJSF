package core;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import jobTemplates.Task;

import org.apache.log4j.Logger;

import utils.Constants;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IdGenerator;

public class Client extends Thread{
	static Logger log = Logger.getLogger(Client.class.getName());
	static long startTime = 0L;
	static long endTime = 0L;
	static HazelcastInstance client=null;
	static int numTasks = 0;
	static long id=0L;

	public static void main(String[] args) {
//		AWSCredentials credentials = 
//				  new PropertiesCredentials(
//				         AwsConsoleApp.class.getResourceAsStream("AwsCredentials.properties"));
//		amazonEC2Client = 
//				  new AmazonEC2Client(credentials);
//		amazonEC2Client.setEndpoint("ec2.us-west-2.amazonaws.com");
		//clientID=staticClientID++;
		Client t = new Client();
		Properties prop = new Properties();
		int sleepTime = 0;
		String addressList;
		int poolSize = 0;
		AtomicNumber numExec = null;
		long currNumExec = 0;
		log.debug(new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime()));
		try {
			prop.load(new FileInputStream("DJSF.properties"));
			log.info("DJSF.properties");
			numTasks = Integer.parseInt(prop.getProperty("numTasks"));
			log.info("numTasks=" + numTasks);
			poolSize = Integer.parseInt(prop.getProperty("poolSize"));
			log.info("poolSize=" + poolSize);
			sleepTime = Integer.parseInt(prop.getProperty("sleepTime"));
			log.info("sleepTime=" + sleepTime);
			addressList = prop.getProperty("DistributedQueueAddress");
			log.info("DistributedQueueAddress=" + addressList);
			ClientConfig clientConfig = new ClientConfig();
			clientConfig.addAddress(addressList.split(","));
			client = HazelcastClient
					.newHazelcastClient(clientConfig);
			numExec = client.getAtomicNumber(Constants.NUM_EXEC);
			do
			{
				currNumExec = numExec.get();
				log.info("numExec=" + currNumExec);
				if (currNumExec == 0)
				{
					log.info("Sleep for 1 min");
					Thread.sleep(60000);
				}
			}while (currNumExec==0);
			IdGenerator idGenerator = client.getIdGenerator(Constants.HZCLIENT);
			id = idGenerator.newId();
			log.debug("Client ID = "+id);
			BlockingQueue<Task> q = client.getQueue(Constants.REQ_QUEUE);
			startTime = System.nanoTime();
			log.debug("Start Time="+startTime+" ns");
			for (int i = 0; i < numTasks; i++) {
				q.put(new Task(id, sleepTime));
			}
			//Check results in a separate thread and print them
			t.start();
			t.join(Constants.MAX_WAIT_TIME);
			log.debug("End Time="+endTime+" ns");
			if (currNumExec != numExec.get())
			{
				log.debug("numExec changed from "+currNumExec+" to "+numExec.get());
			}
			else
			{
				log.debug("numExec = "+currNumExec);
			}
			double diff = ((double)(endTime - startTime)/1000000000);
			log.debug("Time taken = "+diff+" s.");
			double expectedDiff = (double)(numTasks*sleepTime)/(currNumExec*poolSize);
			log.debug("Time expected = "+expectedDiff+" s.");
			client.getLifecycleService().shutdown();
			return;
		} catch (IOException e) {
			log.error(e);
		} catch (InterruptedException e) {
			log.error(e);
		}
	}
	@Override
	public void run() {
		int nTask = numTasks;
		if (client == null)
			return;
		try {
			BlockingQueue<Long> result_q = client.getQueue(Constants.RSP_QUEUE);
			while(nTask>0)
			{
				Long result = result_q.poll(Constants.MAX_WAIT_TIME, TimeUnit.MILLISECONDS);
				nTask--;
				log.info("TaskID "+result+" completed.");
			}
			endTime=System.nanoTime();
		} catch (InterruptedException e) {
			log.error(e);
		}
		
	}

}
