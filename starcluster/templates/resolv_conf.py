resolv_conf_template = """\
# Created and managed by starcluster
nameserver {master_ip:}
search {cluster_tag:}
"""
