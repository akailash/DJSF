package jobTemplates;

import java.io.Serializable;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

public class Task implements Callable<Long>,Serializable {
	static Logger log = Logger.getLogger(Task.class.getName());

	public Task(long clientID, int sleepTime)
	{
		this.sleepTime=sleepTime*1000; //convert to milliseconds
		this.taskID=System.identityHashCode(this);
		this.clientID = clientID;
		this.setExecID(0);
	}
	// serialized id
	private static final long serialVersionUID = 5818183120348936717L;
	private int sleepTime;
	private long taskID;
	private long clientID;
	private long execID;

	/**
	 * @param args
	 */
	@Override
	public Long call() throws Exception {
		try {
			log.info("Executing task "+taskID+" for client "+clientID);
		    Thread.sleep(sleepTime);
		} catch(InterruptedException ex) {
		    Thread.currentThread().interrupt();
		    return 0L;
		}
		log.info("Completed task "+taskID+" for client "+clientID);
		return taskID;
	}

	public long getExecID() {
		return execID;
	}

	public void setExecID(long execID) {
		this.execID = execID;
	}

}
