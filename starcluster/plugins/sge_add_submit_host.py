from starcluster import clustersetup
from starcluster.logger import log


class SgeAddSubmitHostPlugin(clustersetup.DefaultClusterSetup):

    def __init__(self):
        pass

    def run(self, nodes, master, user, user_shell, volumes, hostname,
            private_ip):
        log.info("Running SGE Add Submit Host plugin")

        log.info("Cleaning /etc/hosts from {}".format(private_ip))
        master.ssh.remove_lines_from_file('/etc/hosts', private_ip)

        log.info("Cleaning /etc/hosts from {}".format(hostname))
        master.ssh.remove_lines_from_file('/etc/hosts', hostname)

        log.info("Adding {} {} to /etc/hosts".format(private_ip, hostname))
        host_file = self.ssh.remote_file('/etc/hosts', 'a')
        print >> host_file, "{} {}".format(private_ip, hostname)
        host_file.close()

        log.info("Forcing dnsmasq restart")
        master.ssh.execute("pkill -HUP dnsmasq")

        log.info("Adding host to SGE")
        master.ssh.execute('qconf -as %s' % hostname)
