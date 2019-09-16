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

import IPython
import folium

def _folium_map_formatter(map):
    """ inputs a folium.Map object and returns html of rendered map """
    
    import base64
    html_source = map.get_root().render()
    b64_source = base64.b64encode(
        bytes(html_source.encode('utf-8'))
        ).decode('utf-8')

    source_blob = '<iframe src="data:text/html;charset=utf-8;base64,{}" allowfullscreen="" webkitallowfullscreen="" mozallowfullscreen="" style="position:relative;width:100%;height:500px"></iframe>' 
    return source_blob.format(b64_source)


if IPython.get_ipython() is not None:
    ip = IPython.get_ipython()
    md_formatter = ip.display_formatter.formatters['text/markdown']
    md_formatter.for_type(folium.Map, _folium_map_formatter)