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


class PegdownMarkdownFormatter(PwebPandocFormatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # Pegdown doesn't support the width and label options.
    def make_figure_string(self, figname, width, label, caption=""):
        return "![%s](%s)" % (caption, figname)
