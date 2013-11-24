package core;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import jobTemplates.Task;

import org.apache.log4j.Logger;

import utils.Constants;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IdGenerator;

public class Executor {

	/**
	 * @param args
	 */
	static Logger log = Logger.getLogger(Executor.class.getName());
	static long id=0L;

	public static void main(String[] args) {
		HazelcastInstance client=null;
		Properties prop = new Properties();
		String addressList;
		ExecutorService pool = null;
		int poolSize = 0;
		int counter = 0;
		int waitcount = 0;
		List<Future<Long>> list = new ArrayList<Future<Long>>();
		AtomicNumber numExec = null;
		log.debug(new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime()));
		try {
			prop.load(new FileInputStream("DJSF.properties"));
			addressList = prop.getProperty("DistributedQueueAddress");
			log.info("DistributedQueueAddress=" + addressList);
			poolSize = Integer.parseInt(prop.getProperty("poolSize"));
			pool = Executors.newFixedThreadPool(poolSize);
			ClientConfig clientConfig = new ClientConfig();
			clientConfig.addAddress(addressList.split(","));
			client = HazelcastClient
					.newHazelcastClient(clientConfig);
			numExec = client.getAtomicNumber(Constants.NUM_EXEC);
			log.info("++numExec ="+numExec.incrementAndGet());
			IdGenerator idGenerator = client.getIdGenerator(Constants.HZCLIENT);
			id = idGenerator.newId();
			log.debug("Executor ID = "+id);
			BlockingQueue<Task> req_q = client.getQueue(Constants.REQ_QUEUE);
			//BlockingQueue<Task> sub_q = client.getQueue(Constants.SUB_QUEUE);
			BlockingQueue<Long> rsp_q = client.getQueue(Constants.RSP_QUEUE);
			while (waitcount<Constants.MAX_NUM_POLL) {
				Task task = req_q.poll(Constants.MAX_POLL_TIME, TimeUnit.MILLISECONDS);
				if (task ==null)
				{
					waitcount++;
					continue;
				}
				else
				{
					waitcount=0;
				}
				task.setExecID(id);
				//sub_q.put(task);
				Future<Long> submitedTask = pool.submit(task);
				list.add(submitedTask);
				counter++;
				if ((counter == poolSize)||(req_q.isEmpty())) {
					// do not take more/less than poolSize at a time unless queue is empty
					log.info("Counter= " + counter);
					for (Future<Long> future : list) {
						rsp_q.put(future.get());
						counter--;
					}
					list.clear();
				}
			}
		} catch (IOException e) {
			log.error(e);
		} catch (InterruptedException e) {
			log.error(e);
		} catch (ExecutionException e) {
			log.error(e);
		} finally {
			log.info("--numExec ="+numExec.decrementAndGet());
			if (pool != null)
				pool.shutdown();
			if (client != null)
				client.getLifecycleService().shutdown();
		}
	}

}
