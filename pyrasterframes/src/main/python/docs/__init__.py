#
# This software is licensed under the Apache 2 license, quoted below.
#
# Copyright 2019 Astraea, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# [http://www.apache.org/licenses/LICENSE-2.0]
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.
#
# SPDX-License-Identifier: Apache-2.0
#

from pweave import PwebPandocFormatter

# Setuptools/easy_install doesn't properly set the execute bit on the Spark scripts,
# So this preemptively attempts to do it.
def _chmodit():
    try:
        from importlib.util import find_spec
        import os
        module_home = find_spec("pyspark").origin
        print(module_home)
        bin_dir = os.path.join(os.path.dirname(module_home), 'bin')
        for filename in os.listdir(bin_dir):
            try:
                os.chmod(os.path.join(bin_dir, filename), mode=0o555, follow_symlinks=True)
            except OSError:
                pass
    except ImportError:
        pass

_chmodit()


class PegdownMarkdownFormatter(PwebPandocFormatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # Pegdown doesn't support the width and label options.
    def make_figure_string(self, figname, width, label, caption=""):
        return "![%s](%s)" % (caption, figname)
