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

public class DistributedQHazel {
	static Logger log = Logger.getLogger(DistributedQHazel.class.getName());

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


		} catch (FileNotFoundException e) {
			log.error(e);
		}
	}

}
