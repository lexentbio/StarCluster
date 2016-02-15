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
from starcluster.balancers import sge
from starcluster.tests import StarClusterTest
from starcluster.tests.templates import sge_balancer


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
        balancer = sge.SGELoadBalancer(wait_time=900)
        stat = sge.SGEStats()
        # stat.parse_qstat(sge_balancer.qstat_xml)
        # stat.parse_qhost(sge_balancer.loaded_qhost_xml)
        balancer._stat = stat
        balancer.max_nodes = 100
        current_time = '2010-06-18T23:39:25'
        cluster_has_stabilized = True
        total_slots = 0
        avg_job_duration = 0

        def get_remote_time():
            return utils.iso_to_datetime_tuple(current_time)

        def has_cluster_stabilized():
            return cluster_has_stabilized

        def count_total_slots():
            return total_slots

        def avg_job_duration_fct():
            return avg_job_duration

        balancer.get_remote_time = get_remote_time
        balancer.has_cluster_stabilized = has_cluster_stabilized
        stat.count_total_slots = count_total_slots
        stat.avg_job_duration = avg_job_duration_fct

        assert balancer._eval_required_instances() == 0

        stat.jobs = [{
            'job_state': u'pending',
            'queue_name': u'all.q@ip-10-196-142-180.ec2.internal',
            u'JB_job_number': u'1',
            u'JB_owner': u'root',
            u'state': u'qw',
            u'JAT_prio': u'0.55500',
            u'JB_name': u'sleep',
            u'JB_submission_time': u'2010-06-18T23:39:24',
            u'slots': u'1'
        }]

        assert balancer._eval_required_instances() == 1

        def add_host():
            stat.hosts.append({
                u'swap_used': u'0.0',
                u'arch_string': u'lx24-amd64',
                'name': u'domU-12-31-39-0B-C4-61.compute-1.internal',
                u'swap_total': u'0.0',
                u'num_proc': u'32',
                u'mem_used': u'997.4M',
                u'mem_total': u'7.0G',
                u'load_avg': u'8.32'
            })

        add_host()
        total_slots += 32

        def set_job_to_running(idx):
            del stat.jobs[idx]['JB_submission_time']
            stat.jobs[idx].update({
                'JAT_start_time': '2010-06-18T23:39:24',
                'job_state': 'running',
                'state': 'r'
            })

        set_job_to_running(0)
        assert balancer._eval_required_instances() == 0

        def add_pending_job():
            stat.jobs.append({
                'job_state': u'pending',
                'queue_name': u'all.q@ip-10-196-142-180.ec2.internal',
                u'JB_job_number': u'1',
                u'JB_owner': u'root',
                u'state': u'qw',
                u'JAT_prio': u'0.55500',
                u'JB_name': u'sleep',
                u'JB_submission_time': u'2010-06-18T23:39:24',
                u'slots': u'1'
            })

        add_pending_job()

        # job not waiting for long enough
        assert balancer._eval_required_instances() == 0

        # waiting for long enough
        current_time = '2010-06-18T23:59:25'
        assert balancer._eval_required_instances() == 1

        # avg_job_duration has no effect at that point
        avg_job_duration = 3600
        assert balancer._eval_required_instances() == 1
        avg_job_duration = 1
        assert balancer._eval_required_instances() == 1

        cluster_has_stabilized = False
        assert balancer._eval_required_instances() == 0

        add_pending_job()
        cluster_has_stabilized = True
        for d in [0, 3600, 999999]:
            avg_job_duration = d
            assert balancer._eval_required_instances() == 1

        balancer.add_nodes_per_iteration = 10
        assert balancer._eval_required_instances() == 2

        for _ in xrange(4):
            add_pending_job()

        # now there are 6 waiting jobs
        assert balancer._eval_required_instances() == 6

        avg_job_duration = 1800
        assert balancer._eval_required_instances() == 3

        avg_job_duration = 1700
        assert balancer._eval_required_instances() == 3

        avg_job_duration = 100
        assert balancer._eval_required_instances() == 1

        add_host()
        total_slots += 32
        set_job_to_running(1)
        set_job_to_running(2)

        # now there are 4 waiting jobs, 3 running ones, avg slot per job is
        # 63 / 3 = 21
        # 4 jobs * 21  = 84
        # ceil(84 / 32) = 3
        avg_job_duration = 3600
        assert balancer._eval_required_instances() == 3

        avg_job_duration = 1200
        assert balancer._eval_required_instances() == 1
