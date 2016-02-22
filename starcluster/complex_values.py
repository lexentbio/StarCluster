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


class ComplexValues(dict):
    def __hash__(self):
        return hash(tuple(sorted(self.items())))

    def fits_in(self, other):
        for k, v in self.iteritems():
            if k not in other:
                return False
            if other[k] < v:
                return False
        return True

    def fits_in_any(self, others):
        for other in others:
            if self.fits_in(other):
                return True
        return False
