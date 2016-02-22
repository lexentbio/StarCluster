# Copyright 2016 Mich
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
from starcluster.complex_values import ComplexValues
from starcluster.tests import StarClusterTest


class TestSGELoadBalancer(StarClusterTest):

    def test_complex_values(self):
        node_cfg = ComplexValues({'ram': 10})
        job_req = ComplexValues({'ram': 5})
        assert job_req.fits_in(node_cfg)

        job_req['ram'] = 10
        assert job_req.fits_in(node_cfg)

        job_req['ram'] = 10.00001
        assert not job_req.fits_in(node_cfg)

        node_cfg['cores'] = 4
        assert not job_req.fits_in(node_cfg)

        job_req['ram'] = 1
        assert job_req.fits_in(node_cfg)

        job_req['cores'] = 5
        assert not job_req.fits_in(node_cfg)

        job_req['ram'] = 12
        assert not job_req.fits_in(node_cfg)

        job_req = ComplexValues()
        assert job_req.fits_in(node_cfg)

        job_req['VGA'] = 1
        assert not job_req.fits_in(node_cfg)
