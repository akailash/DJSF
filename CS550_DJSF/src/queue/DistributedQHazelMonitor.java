package queue;

import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Scanner;
import java.util.Set;

import org.apache.log4j.Logger;

import utils.Constants;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.AtomicNumber;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;

public class DistributedQHazelMonitor {
	static Logger log = Logger.getLogger(DistributedQHazelMonitor.class.getName());

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Config cfg;
		log.debug(new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar
				.getInstance().getTime()));
		try {
			cfg = new XmlConfigBuilder("hazelcast.xml").build();
			HazelcastInstance hz = Hazelcast.newHazelcastInstance(cfg);
			Cluster cluster = hz.getCluster();
			cluster.addMembershipListener(new MembershipListener() {
				public void memberAdded(MembershipEvent memberEvent) {
					log.info("MemberAdded " + memberEvent);
				}

				public void memberRemoved(MembershipEvent memberEvent) {
					log.info("MemberRemoved " + memberEvent);
				}
			});
			Member localMember = cluster.getLocalMember();
			log.info("my inetAddress= " + localMember.getInetAddress());
			Set<Member> setMembers = cluster.getMembers();
			for (Member member : setMembers) {
				log.info("isLocalMember " + member.localMember());
				log.info("member.inetaddress " + member.getInetAddress());
				log.info("member.port " + member.getPort());
			}
			boolean breakwhile = false;
			Scanner myScan = new Scanner(System.in);
			while (!breakwhile) {
				AtomicNumber numExec = hz.getAtomicNumber(Constants.NUM_EXEC);
				long localnumExec = numExec.get();
				System.out.println("Number of Executers : " + localnumExec);
				System.out.println("Press 1 to scan number again ");
				System.out.println("Press 2 to reset ");
				System.out.println("Press 3 to exit ");
				String choice = myScan.nextLine();
				switch (choice) {
				case "1":
					break;
				case "2":
					numExec.set(0);
					break;
				case "3":
					breakwhile = true;
					break;
				}

			}
		} catch (FileNotFoundException e) {
			log.error(e);
		}
	}

}
