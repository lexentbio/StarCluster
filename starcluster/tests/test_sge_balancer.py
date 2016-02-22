# Copyright 2009-2014 Justin Riley
#
# This file is part of StarCluster.
#
# StarCluster is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# StarCluster is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with StarCluster. If not, see <http://www.gnu.org/licenses/>.

import iso8601
import datetime

from starcluster import utils
from starcluster.complex_values import ComplexValues
from starcluster.balancers import sge
from starcluster.balancers.sge import SGEJob
from starcluster.tests import StarClusterTest
from starcluster.tests.templates import sge_balancer


class SGEBalancerMock(object):

    def __init__(self, sge_balancer):
        self.current_time = '2010-06-18T23:39:25'
        self.has_cluster_stabilized = True
        self.count_total_slots = 0
        self.avg_job_duration = 0
        self.has_cluster_stabilized = True

        def get_remote_time_fct():
            return utils.iso_to_datetime_tuple(self.current_time)

        def has_cluster_stabilized_fct():
            return self.has_cluster_stabilized

        def count_total_slots_fct():
            return self.count_total_slots

        def avg_job_duration_fct():
            return self.avg_job_duration

        sge_balancer.max_nodes = 100
        sge_balancer.get_remote_time = get_remote_time_fct
        sge_balancer.has_cluster_stabilized = has_cluster_stabilized_fct
        sge_balancer.stat.count_total_slots = count_total_slots_fct
        sge_balancer.stat.avg_job_duration = avg_job_duration_fct


class TestSGELoadBalancer(StarClusterTest):

    def test_qhost_parser(self):
        stat = sge.SGEStats()
        host_hash = stat.parse_qhost(sge_balancer.qhost_xml)
        assert len(host_hash) == 3
        assert len(host_hash) == stat.count_hosts()

    def test_loaded_qhost_parser(self):
        stat = sge.SGEStats()
        host_hash = stat.parse_qhost(sge_balancer.loaded_qhost_xml)
        assert len(host_hash) == 10
        assert len(host_hash) == stat.count_hosts()

    def test_qstat_parser(self):
        stat = sge.SGEStats()
        stat_hash = stat.parse_qstat(sge_balancer.qstat_xml)
        assert len(stat_hash) == 23
        assert stat.first_job_id == 1
        assert stat.last_job_id == 23
        assert len(stat.get_queued_jobs()) == 20
        assert len(stat.get_running_jobs()) == 3
        assert stat.num_slots_for_job(21) == 1
        oldest = datetime.datetime(2010, 6, 18, 23, 39, 14,
                                   tzinfo=iso8601.iso8601.UTC)
        assert stat.oldest_queued_job_age() == oldest
        assert len(stat.queues) == 3

    def test_qacct_parser(self):
        stat = sge.SGEStats()
        now = utils.get_utc_now()
        self.jobstats = stat.parse_qacct(sge_balancer.qacct_txt, now)
        assert stat.avg_job_duration() == 90
        assert stat.avg_wait_time() == 263

    def test_loaded_qstat_parser(self):
        stat = sge.SGEStats()
        stat_hash = stat.parse_qstat(sge_balancer.loaded_qstat_xml)
        assert len(stat_hash) == 192
        assert stat.first_job_id == 385
        assert stat.last_job_id == 576
        assert len(stat.get_queued_jobs()) == 188
        assert len(stat.get_running_jobs()) == 4
        assert stat.num_slots_for_job(576) == 20
        oldest = datetime.datetime(2010, 7, 8, 4, 40, 32,
                                   tzinfo=iso8601.iso8601.UTC)
        assert stat.oldest_queued_job_age() == oldest
        assert len(stat.queues) == 10
        assert stat.count_total_slots() == 80
        stat.parse_qhost(sge_balancer.loaded_qhost_xml)
        # assert stat.slots_per_host() == 8

    def test_eval_required_instances(self):
        node_complex_values = {'someNode': {}}
        balancer = sge.SGELoadBalancer(wait_time=900,
                                       node_complex_values=node_complex_values)
        stat = sge.SGEStats()
        balancer._stat = stat
        balancer_mock = SGEBalancerMock(balancer)

        assert balancer._eval_required_instances() == (0, [])

        stat.jobs = [SGEJob({
            'job_state': u'pending',
            'queue_name': u'all.q@ip-10-196-142-180.ec2.internal',
            u'JB_job_number': u'1',
            u'JB_owner': u'root',
            u'state': u'qw',
            u'JAT_prio': u'0.55500',
            u'JB_name': u'sleep',
            u'JB_submission_time': u'2010-06-18T23:39:24',
            u'slots': u'1'
        })]

        assert balancer._eval_required_instances()[0] == 1

        def add_host(name):
            stat.hosts[name] = {
                u'swap_used': u'0.0',
                u'arch_string': u'lx24-amd64',
                'name': u'domU-12-31-39-0B-C4-61.compute-1.internal',
                u'swap_total': u'0.0',
                u'num_proc': u'32',
                u'mem_used': u'997.4M',
                u'mem_total': u'7.0G',
                u'load_avg': u'8.32'
            }

        add_host('host1')
        balancer_mock.count_total_slots += 32

        def set_job_to_running(idx):
            del stat.jobs[idx]['JB_submission_time']
            stat.jobs[idx].update({
                'JAT_start_time': '2010-06-18T23:39:24',
                'job_state': 'running',
                'state': 'r'
            })

        set_job_to_running(0)
        assert balancer._eval_required_instances() == (0, [])

        def add_pending_job():
            stat.jobs.append(SGEJob({
                'job_state': u'pending',
                'queue_name': u'all.q@ip-10-196-142-180.ec2.internal',
                u'JB_job_number': unicode(len(stat.jobs) + 1),
                u'JB_owner': u'root',
                u'state': u'qw',
                u'JAT_prio': u'0.55500',
                u'JB_name': u'sleep',
                u'JB_submission_time': u'2010-06-18T23:39:24',
                u'slots': u'1'
            }))

        add_pending_job()

        # job not waiting for long enough
        assert balancer._eval_required_instances() == (0, [])

        # waiting for long enough
        balancer_mock.current_time = '2010-06-18T23:59:25'
        assert balancer._eval_required_instances()[0] == 1

        # avg_job_duration has no effect at that point
        balancer_mock.avg_job_duration = 3600
        assert balancer._eval_required_instances()[0] == 1
        balancer_mock.avg_job_duration = 1
        assert balancer._eval_required_instances()[0] == 1

        balancer_mock.has_cluster_stabilized = False
        assert balancer._eval_required_instances() == (0, [])

        add_pending_job()
        balancer_mock.has_cluster_stabilized = True
        for d in [0, 3600, 999999]:
            balancer_mock.avg_job_duration = d
            assert balancer._eval_required_instances()[0] == 1

        balancer.add_nodes_per_iteration = 10
        assert balancer._eval_required_instances()[0] == 2

        for _ in xrange(4):
            add_pending_job()

        # now there are 6 waiting jobs
        assert balancer._eval_required_instances()[0] == 6

        if False:
            # avg_job_duration was removed as a weithg factor because the
            # metric returned by OGSStat is currently 8 seconds, which would
            # mean we would end up adding nodes one by one.
            balancer_mock.avg_job_duration = 1800
            assert balancer._eval_required_instances()[0] == 3

            balancer_mock.avg_job_duration = 1700
            assert balancer._eval_required_instances()[0] == 3

            balancer_mock.avg_job_duration = 100
            assert balancer._eval_required_instances()[0] == 1

            add_host('host2')
            balancer_mock.count_total_slots += 32
            set_job_to_running(1)
            set_job_to_running(2)

            # now there are 4 waiting jobs, 3 running ones, avg slot per job is
            # 64 / 3 = 21.3
            # 4 jobs * 21.3  = 85.3
            # ceil(85.3 / 32) = 3
            balancer_mock.avg_job_duration = 3600
            assert balancer._eval_required_instances()[0] == 3

            balancer_mock.avg_job_duration = 1200
            assert balancer._eval_required_instances()[0] == 1

    def test_loaded_qstat_parser_complex_values(self):
        stat = sge.SGEStats()
        with open('starcluster/tests/templates/qstat2.xml', 'rt') as f:
            qstat2_str = f.read()
        stat.parse_qstat(qstat2_str)

        first_running_job = stat.get_running_jobs()[0]
        assert first_running_job['hard_request']['da_mem_gb'] == 32
        assert first_running_job['hard_request']['da_slots'] == 32

        last_queued_job = stat.get_queued_jobs()[1]
        assert last_queued_job['hard_request']['da_mem_gb'] == 70
        assert last_queued_job['hard_request']['da_slots'] == 16

    def test_queued_job_request_too_many_resources(self):
        stat = sge.SGEStats()
        node_complex_values = {
            'smallNode': ComplexValues({
                'da_mem_gb': 1,
                'da_slots': 1
            }),
            'mediumNode': ComplexValues({
                'da_mem_gb': 8,
                'da_slots': 4
            })
        }

        balancer = sge.SGELoadBalancer(
            wait_time=900, supported_complex_values=['da_mem_gb', 'da_slots'],
            node_complex_values=node_complex_values)
        balancer._stat = stat
        balancer_mock = SGEBalancerMock(balancer)

        # waiting for long enough
        balancer_mock.current_time = '2010-06-18T23:59:25'

        stat.jobs.append(SGEJob({
            'job_state': u'pending',
            'queue_name': u'all.q@ip-10-196-142-180.ec2.internal',
            u'JB_job_number': unicode(len(stat.jobs) + 1),
            u'JB_owner': u'root',
            u'state': u'qw',
            u'JAT_prio': u'0.55500',
            u'JB_name': u'sleep',
            u'JB_submission_time': u'2010-06-18T23:39:24',
            u'slots': u'1',
            u'hard_request': {
                'da_slots': 1
            }
        }))
        res = balancer.get_jobs_instances_support(stat.jobs)
        assert len(res['unfulfillable']) == 0
        assert balancer._eval_required_instances() == \
            (1, ['smallNode', 'mediumNode'])

        stat.jobs[0]['hard_request']['da_slots'] = 10
        res = balancer.get_jobs_instances_support(stat.jobs)
        assert len(res['unfulfillable']) == 1
        assert balancer._eval_required_instances()[0] == 0

        stat.jobs.append(SGEJob({
            'job_state': u'pending',
            'queue_name': u'all.q@ip-10-196-142-180.ec2.internal',
            u'JB_job_number': unicode(len(stat.jobs) + 1),
            u'JB_owner': u'root',
            u'state': u'qw',
            u'JAT_prio': u'0.55500',
            u'JB_name': u'sleep',
            u'JB_submission_time': u'2010-06-18T23:39:24',
            u'slots': u'1',
            u'hard_request': {
                'da_slots': 1
            }
        }))
        assert balancer._eval_required_instances() == \
            (1, ['smallNode', 'mediumNode'])

        stat.jobs[1]['hard_request']['irrelevant'] = 999
        assert balancer._eval_required_instances() == \
            (1, ['smallNode', 'mediumNode'])

        stat.jobs[1]['hard_request']['da_mem_gb'] = 1
        assert balancer._eval_required_instances() == \
            (1, ['smallNode', 'mediumNode'])

        stat.jobs[1]['hard_request']['da_mem_gb'] = 8
        assert balancer._eval_required_instances() == (1, ['mediumNode'])

        stat.jobs[1]['hard_request']['da_mem_gb'] = 8.1
        assert balancer._eval_required_instances()[0] == 0

    def test_find_nodes_for_removal_base(self):
        """
        Base means no mixed node types, no complex values
        """
        class ClusterMock(object):
            pass

        class NodeMock(object):
            def __init__(self, alias, launch_time):
                self.alias = alias
                self.id = alias
                self.launch_time = launch_time

            def is_master(self):
                return False

        stat = sge.SGEStats()
        balancer = sge.SGELoadBalancer(wait_time=900)
        balancer._stat = stat
        balancer_mock = SGEBalancerMock(balancer)
        cluster_mock = ClusterMock()
        cluster_mock.running_nodes = [NodeMock('host1', '2010-06-18T23:00:00')]
        balancer._cluster = cluster_mock

        stat.hosts['host1'] = {
            u'swap_used': u'0.0',
            u'arch_string': u'lx24-amd64',
            'name': u'host1',
            u'swap_total': u'0.0',
            u'num_proc': u'32',
            u'mem_used': u'997.4M',
            u'mem_total': u'7.0G',
            u'load_avg': u'8.32'
        }
        remove_nodes = balancer._find_nodes_for_removal()
        assert not remove_nodes, "Too soon, nothing should be removed"

        # waiting for long enough
        balancer_mock.current_time = '2010-06-18T23:59:25'
        remove_nodes = balancer._find_nodes_for_removal()
        assert remove_nodes[0].alias == 'host1'

        stat.jobs.append(SGEJob({
            'job_state': u'pending',
            'queue_name': u'all.q@host1',
            u'JB_job_number': unicode(len(stat.jobs) + 1),
            u'JB_owner': u'root',
            u'state': u'qw',
            u'JAT_prio': u'0.55500',
            u'JB_name': u'sleep',
            u'JB_submission_time': u'2010-06-18T23:39:24',
            u'slots': u'1'
        }))
        remove_nodes = balancer._find_nodes_for_removal()
        assert not remove_nodes, "job running on it, nothing should be removed"

        name = 'host2'
        cluster_mock.running_nodes.append(NodeMock(name,
                                                   '2010-06-18T23:00:00'))
        stat.hosts[name] = {
            u'swap_used': u'0.0',
            u'arch_string': u'lx24-amd64',
            'name': name,
            u'swap_total': u'0.0',
            u'num_proc': u'32',
            u'mem_used': u'997.4M',
            u'mem_total': u'7.0G',
            u'load_avg': u'8.32'
        }
        remove_nodes = balancer._find_nodes_for_removal()
        assert remove_nodes[0].alias == 'host2'

    def test_find_nodes_for_removal_complex_values(self):
        class ClusterMock(object):
            pass

        class NodeMock(object):
            def __init__(self, alias, launch_time):
                self.alias = alias
                self.id = alias
                self.launch_time = launch_time

            def is_master(self):
                return False

        stat = sge.SGEStats()
        node_complex_values = {
            'smallNode': ComplexValues({
                'da_mem_gb': 1,
                'da_slots': 1
            }),
            'mediumNode': ComplexValues({
                'da_mem_gb': 8,
                'da_slots': 4
            })
        }

        balancer = sge.SGELoadBalancer(
            wait_time=900, supported_complex_values=['da_mem_gb', 'da_slots'],
            node_complex_values=node_complex_values)
        balancer._stat = stat
        balancer_mock = SGEBalancerMock(balancer)
        cluster_mock = ClusterMock()
        name = 'host1'
        cluster_mock.running_nodes = [NodeMock(name, '2010-06-18T23:00:00')]
        balancer._cluster = cluster_mock

        stat.hosts[name] = {
            u'swap_used': u'0.0',
            u'arch_string': u'lx24-amd64',
            'name': name,
            u'swap_total': u'0.0',
            u'num_proc': u'32',
            u'mem_used': u'997.4M',
            u'mem_total': u'7.0G',
            u'load_avg': u'8.32',
            u'available_complex_values': ComplexValues({
                'da_mem_gb': 7.8,  # usually lower than what is configured
                'da_slots': 4
            })
        }
        remove_nodes = balancer._find_nodes_for_removal(
            unusable_types=['smallNode', 'mediumNode'])
        assert not remove_nodes, "Too soon, nothing should be removed"

        # waiting for long enough
        balancer_mock.current_time = '2010-06-18T23:59:25'
        remove_nodes = balancer._find_nodes_for_removal(
            unusable_types=['smallNode', 'mediumNode'])
        assert remove_nodes[0].alias == 'host1'

        remove_nodes = balancer._find_nodes_for_removal(
            unusable_types=['smallNode'])
        assert not remove_nodes, 'bigger node should not be removed'

        name = 'host2'
        cluster_mock.running_nodes.append(NodeMock(name,
                                                   '2010-06-18T23:00:00'))
        stat.hosts[name] = {
            u'swap_used': u'0.0',
            u'arch_string': u'lx24-amd64',
            'name': name,
            u'swap_total': u'0.0',
            u'num_proc': u'32',
            u'mem_used': u'997.4M',
            u'mem_total': u'0.99G',
            u'load_avg': u'8.32',
            u'available_complex_values': ComplexValues({
                'da_mem_gb': 0.98,  # usually lower than what is configured
                'da_slots': 1
            })
        }

        remove_nodes = balancer._find_nodes_for_removal(
            unusable_types=['smallNode'])
        assert remove_nodes[0].alias == 'host2'

        remove_nodes = balancer._find_nodes_for_removal(
            unusable_types=['smallNode', 'mediumNode'])
        assert remove_nodes[0].alias == 'host1'
        assert remove_nodes[1].alias == 'host2'

        stat.jobs.append(SGEJob({
            'job_state': u'pending',
            'queue_name': u'all.q@host2',
            u'JB_job_number': unicode(len(stat.jobs) + 1),
            u'JB_owner': u'root',
            u'state': u'qw',
            u'JAT_prio': u'0.55500',
            u'JB_name': u'sleep',
            u'JB_submission_time': u'2010-06-18T23:39:24',
            u'slots': u'1',
            u'hard_request': {
                'da_slots': 1
            }
        }))
        remove_nodes = balancer._find_nodes_for_removal(
            unusable_types=['smallNode'])
        assert not remove_nodes, 'Node busy, should not remove'

        remove_nodes = balancer._find_nodes_for_removal(
            unusable_types=['smallNode', 'mediumNode'])
        assert remove_nodes[0].alias == 'host1'

        # specifying only the larget type does the same
        remove_nodes = balancer._find_nodes_for_removal(
            unusable_types=['mediumNode'])
        assert remove_nodes[0].alias == 'host1'

        stat.jobs[0]['queue_name'] = u'all.q@host1'
        remove_nodes = balancer._find_nodes_for_removal(
            unusable_types=['mediumNode'])
        assert remove_nodes[0].alias == 'host2'

        remove_nodes = balancer._find_nodes_for_removal(
            unusable_types=['smallNode'])
        assert remove_nodes[0].alias == 'host2'

        stat.jobs.append(SGEJob({
            'job_state': u'pending',
            'queue_name': u'all.q@host2',
            u'JB_job_number': unicode(len(stat.jobs) + 1),
            u'JB_owner': u'root',
            u'state': u'qw',
            u'JAT_prio': u'0.55500',
            u'JB_name': u'sleep',
            u'JB_submission_time': u'2010-06-18T23:39:24',
            u'slots': u'1',
            u'hard_request': {
                'da_slots': 1
            }
        }))

        remove_nodes = balancer._find_nodes_for_removal(
            unusable_types=['smallNode'])
        assert not remove_nodes, 'Nodes busy, should not remove'

        remove_nodes = balancer._find_nodes_for_removal(
            unusable_types=['smallNode', 'mediumNode'])
        assert not remove_nodes, 'Nodes busy, should not remove'
