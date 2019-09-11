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

import os
from pweave import PwebPandocFormatter


def docs_dir():
    return os.path.dirname(os.path.realpath(__file__))


# This is temporary until we port to run on web assets.
def resource_dir():
    # we may consider using gitpython which I think would be appropriate in the context of building docs
    # see https://stackoverflow.com/a/41920796/2787937
    here = docs_dir()
    test_resource = os.path.realpath(os.path.join(here, '..', '..', '..', 'src', 'test', 'resources'))

    return test_resource


def resource_dir_uri():
    return 'file://' + resource_dir()


def folium_map(vector_data):
    from folium import Map, GeoJson

    import base64
    from uuid import uuid4

    # use chunck option results='paradox' with this; also recommend echo=False
    gl = GeoJson(vector_data)
    m = Map()
    m.fit_bounds(gl.get_bounds())
    m.add_child(gl)

    temp_fname = str(uuid4())
    temp_folium = 'docs/static/{}.html'.format(temp_fname)
    m.save(temp_folium)

    with open(temp_folium, 'rb') as f:
        b64 = base64.b64encode(f.read())
    with open('docs/static/{}.md'.format(temp_fname), 'w') as md:
        md.write('<iframe src="data:text/html;charset=utf-8;base64,{}" allowfullscreen="" webkitallowfullscreen="" mozallowfullscreen="" style="position:relative;width:100%;height:500px"></iframe>'.format(b64.decode('utf-8')))
    
    return '@@include[folium_map](static/{}.md)'.format(temp_fname)


class PegdownMarkdownFormatter(PwebPandocFormatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # Pegdown doesn't support the width and label options.
    def make_figure_string(self, figname, width, label, caption=""):
        return "![%s](%s)" % (caption, figname)
