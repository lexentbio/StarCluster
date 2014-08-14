import re
from starcluster import clustersetup
from starcluster.logger import log


class DatacraticPostPlugin(clustersetup.DefaultClusterSetup):

    _dcePath = "/usr/bin/datacraticCopyEditor"

    def __init__(self, **kwargs):
        pass

    def run(self, nodes, master, user, user_shell, volumes):
        self._create_dce(master, True)
        log.info("Setting master to 0 slot")
        self._set_node_slots(master, master.alias, 0)
        self._create_complex_values(master)
        self._update_sge_msconf(master)
        log.info("******")
        log.info("You need to run salt over the master node before adding "
                 "slave nodes.")
        log.info("******")

    def on_add_node(self, node, nodes, master, user, user_shell, volumes):
        self._create_dce(master)
        log.info("Setting " + node.alias + " to 1 slot")
        self._set_node_slots(master, node.alias, 1)
        log.info("Configuring complex values for " + node.alias)
        self._update_complex_values(master, node)

    def on_remove_node(self, node, nodes, master, user, user_shell, volumes):
        # this is done to fix an issue where, even if a node is dead, after
        # related jobs are deleted OGS tries to assign it new jobs and when
        # we try to remove the node from OGS configs, it returns an error
        # message "Host object "nodeX" is still referenced in cluster queue
        log.info("Setting " + node.alias + " to 0 slot")
        self._set_node_slots(master, node.alias, 0)

    def clean_cluster(self, nodes, master, user, user_shell, volumes):
        pass

    def _create_dce(self, master, force=False):
        if not master.ssh.path_exists(self._dcePath) or force:
            #TODO: nice hack, complete it by doing the entire stuff remotely
            dce = master.ssh.remote_file(self._dcePath, "w")
            dce.write("#!/bin/bash\ncp $1 $DCE_DEST\n")
            dce.close()
            master.ssh.execute("chmod +x " + self._dcePath)

    def _set_node_slots(self, master, node_alias, num_slots):
        qconfPath = "/root/queueconfig.qconf"
        #with our copy editor, the current config is printed to a file
        master.ssh.execute("export EDITOR=" + self._dcePath + "; "
                           + "chmod +x $EDITOR; "
                           + "export DCE_DEST=" + qconfPath + "; "
                           + "qconf -mq all.q")
        qconf = master.ssh.remote_file(qconfPath, "r")
        qconfStr = qconf.read()
        qconf.close()
        search = "\[" + node_alias + "=[\d]+\]"
        replace = "[" + node_alias + "=" + str(num_slots) + "]"
        qconfStr, numReplace = re.subn(search, replace, qconfStr)
        if numReplace == 0:
            log.error("datacratic plugin: failed to set " + str(num_slots) +
                      " slot(s) on node " + node_alias)
        else:
            qconf = master.ssh.remote_file(qconfPath, "w")
            qconf.write(qconfStr)
            qconf.close()
            master.ssh.execute("qconf -Mq " + qconfPath)

    def _create_complex_values(self, master):
        """
        Defines complex values in OGS
        """
        log.info("Creating complex values configuration")
        dest = "/root/qconf_mc.qconf"
        master.ssh.execute("export EDITOR=" + self._dcePath + "; "
                           + "export DCE_DEST=" + dest + "; "
                           + "qconf -mc")
        qconf = master.ssh.remote_file(dest, "r")
        qconfStr = qconf.read()
        qconf.close()
        qconfStr += "da_exclusive da_excl INT <= YES YES 0 0\n"\
            + "da_mem_gb da_mem_gb DOUBLE <= YES YES 0 0\n"\
            + "da_slots da_slots INT <= YES YES 0 0\n"
        qconf = master.ssh.remote_file(dest, "w")
        qconf.write(qconfStr)
        qconf.close()
        master.ssh.execute("qconf -Mc " + dest)

    def _update_sge_msconf(self, master):
        """
        Set the default_runtime to 48:00:00
        """
        log.info("Defining default_runtime")
        dest = "/root/qconf_msconf.qconf"
        master.ssh.execute("export EDITOR=" + self._dcePath + "; "
                            "export DCE_DEST=" + dest + "; "
                            "qconf -msconf", ignore_exit_status=True)
        qconf = master.ssh.remote_file(dest, "r")
        qconf_lines = qconf.readlines()
        qconf.close()
        for i, l in enumerate(qconf_lines):
            if l.startswith('default_duration'):
                del qconf_lines[i]
                break
        qconf_lines.append("default_duration 48:00:00")
        qconf = master.ssh.remote_file(dest, "w")
        qconf.write("\n".join(qconf_lines))
        qconf.close()
        master.ssh.execute("qconf -Msconf " + dest)

    def _update_complex_values(self, master, node):
        """
        Sets complex values values for a node
        """
        master.ssh.execute("qconf -rattr exechost complex_values "\
                           "da_mem_gb="\
                           + str(float(node.memory) / 1000)\
                           + ",da_slots="\
                           + str(node.num_processors)\
                           + ",da_exclusive=1 " + node.alias)

    def recover(self, nodes, master, user, user_shell, volumes):
        pass
