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

import pyrasterframes.rf_types
import numpy as np


def tile_to_png(tile, lower_percentile=1, upper_percentile=99, title=None, fig_size=None):
    """ Provide image of Tile."""
    if tile.cells is None:
        return None

    import io
    from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
    from matplotlib.figure import Figure

    # Set up matplotlib objects
    nominal_size = 4  # approx full size for a 256x256 tile
    if fig_size is None:
        fig_size = (nominal_size, nominal_size)

    fig = Figure(figsize=fig_size)
    canvas = FigureCanvas(fig)
    axis = fig.add_subplot(1, 1, 1)

    arr = tile.cells

    def normalize_cells(cells, lower_percentile=lower_percentile, upper_percentile=upper_percentile):
        assert upper_percentile > lower_percentile, 'invalid upper and lower percentiles'
        lower = np.percentile(cells, lower_percentile)
        upper = np.percentile(cells, upper_percentile)
        cells = np.clip(cells, lower, upper)
        return (cells - lower) / (upper - lower)

    axis.imshow(normalize_cells(arr))
    axis.set_aspect('equal')
    axis.xaxis.set_ticks([])
    axis.yaxis.set_ticks([])

    if title is None:
        axis.set_title('{}, {}'.format(tile.dimensions(), tile.cell_type.__repr__()),
                       fontsize=fig_size[0]*4)  # compact metadata as title
    else:
        axis.set_title(title, fontsize=fig_size[0]*4)  # compact metadata as title

    with io.BytesIO() as output:
        canvas.print_png(output)
        return output.getvalue()


def tile_to_html(tile, fig_size=None):
    """ Provide HTML string representation of Tile image."""
    import base64
    b64_img_html = '<img src="data:image/png;base64,{}" />'
    png_bits = tile_to_png(tile, fig_size)
    b64_png = base64.b64encode(png_bits).decode('utf-8').replace('\n', '')
    return b64_img_html.format(b64_png)


def pandas_df_to_html(df):
    """Provide HTML formatting for pandas.DataFrame with rf_types.Tile in the columns.  """
    import pandas as pd
    # honor the existing options on display
    if not pd.get_option("display.notebook_repr_html"):
        return None

    if len(df) == 0:
        return df._repr_html_()

    tile_cols = []
    for c in df.columns:
        if isinstance(df.iloc[0][c], pyrasterframes.rf_types.Tile):  # if the first is a Tile try formatting
            tile_cols.append(c)

    def _safe_tile_to_html(t):
        if isinstance(t, pyrasterframes.rf_types.Tile):
            return tile_to_html(t, fig_size=(2, 2))
        else:
            # handles case where objects in a column are not all Tile type
            return t.__repr__()

    # dict keyed by column with custom rendering function
    formatter = {c: _safe_tile_to_html for c in tile_cols}

    # This is needed to avoid our tile being rendered as `<img src="only up to fifty char...`
    default_max_colwidth = pd.get_option('display.max_colwidth')  # we'll try to politely put it back
    pd.set_option('display.max_colwidth', -1)
    return_html = df.to_html(escape=False,  # means our `< img` does not get changed to `&lt; img`
                             formatters=formatter,  # apply custom format to columns
                             render_links=True,  # common in raster frames
                             notebook=True,
                             max_rows=pd.get_option("display.max_rows"),  # retain existing options
                             max_cols=pd.get_option("display.max_columns"),
                             show_dimensions=pd.get_option("display.show_dimensions"),
                             )
    pd.set_option('display.max_colwidth', default_max_colwidth)
    return return_html


try:
    from IPython import get_ipython
    # modifications to currently running ipython session, if we are in one; these enable nicer visualization for Pandas
    if get_ipython() is not None:
        import pandas
        html_formatter = get_ipython().display_formatter.formatters['text/html']
        html_formatter.for_type(pandas.DataFrame, pandas_df_to_html)
except ImportError:
    pass
