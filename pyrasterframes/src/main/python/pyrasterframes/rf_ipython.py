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
from functools import partial

import pyrasterframes.rf_types
from pyrasterframes.rf_types import Tile
from shapely.geometry.base import BaseGeometry
from matplotlib.axes import Axes
import numpy as np
from pandas import DataFrame
from typing import Optional, Tuple, Union

_png_header = bytearray([0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A])


def plot_tile(tile: Tile, normalize: bool = True, lower_percentile: float = 1., upper_percentile: float = 99.,
              axis: Optional[Axes] = None, **imshow_args):
    """
    Display an image of the tile

    Parameters
    ----------
    tile: item to plot
    normalize: if True, will normalize the data between using
               lower_percentile and upper_percentile as bounds
    lower_percentile: between 0 and 100 inclusive.
                      Specifies to clip values below this percentile
    upper_percentile: between 0 and 100 inclusive.
                      Specifies to clip values above this percentile
    axis : matplotlib axis object to plot onto. Creates new axis if None
    imshow_args : parameters to pass into matplotlib.pyplot.imshow
                  see https://matplotlib.org/3.1.1/api/_as_gen/matplotlib.pyplot.imshow.html
    Returns
    -------
    created or modified axis object
    """
    if axis is None:
        import matplotlib.pyplot as plt
        axis = plt.gca()

    arr = tile.cells

    def normalize_cells(cells: np.ndarray) -> np.ndarray:
        assert upper_percentile > lower_percentile, 'invalid upper and lower percentiles {}, {}'.format(
            lower_percentile, upper_percentile)
        sans_mask = np.array(cells)
        lower = np.nanpercentile(sans_mask, lower_percentile)
        upper = np.nanpercentile(sans_mask, upper_percentile)
        cells_clipped = np.clip(cells, lower, upper)
        return (cells_clipped - lower) / (upper - lower)

    axis.set_aspect('equal')
    axis.xaxis.set_ticks([])
    axis.yaxis.set_ticks([])

    if normalize:
        cells = normalize_cells(arr)
    else:
        cells = arr

    axis.imshow(cells, **imshow_args)

    return axis


def tile_to_png(tile: Tile, lower_percentile: float = 1., upper_percentile: float = 99., title: Optional[str] = None,
                fig_size: Optional[Tuple[int, int]] = None) -> bytes:
    """ Provide image of Tile."""
    if tile.cells is None:
        return None

    import io
    from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
    from matplotlib.figure import Figure

    # Set up matplotlib objects
    nominal_size = 2
    if fig_size is None:
        fig_size = (nominal_size, nominal_size)

    fig = Figure(figsize=fig_size)
    canvas = FigureCanvas(fig)
    axis = fig.add_subplot(1, 1, 1)

    plot_tile(tile, True, lower_percentile, upper_percentile, axis=axis)
    axis.set_aspect('equal')
    axis.xaxis.set_ticks([])
    axis.yaxis.set_ticks([])

    if title is None:
        axis.set_title('{}, {}'.format(tile.dimensions(), tile.cell_type.__repr__()),
                       fontsize=fig_size[0] * 4)  # compact metadata as title
    else:
        axis.set_title(title, fontsize=fig_size[0] * 4)  # compact metadata as title

    with io.BytesIO() as output:
        canvas.print_png(output)
        return output.getvalue()


def tile_to_html(tile: Tile, fig_size: Optional[Tuple[int, int]] = None) -> str:
    """ Provide HTML string representation of Tile image."""
    import base64
    b64_img_html = '<img src="data:image/png;base64,{}" />'
    png_bits = tile_to_png(tile, fig_size=fig_size)
    b64_png = base64.b64encode(png_bits).decode('utf-8').replace('\n', '')
    return b64_img_html.format(b64_png)


def binary_to_html(blob) -> Union[str, bytearray]:
    """ When using rf_render_png, the result from the JVM is a byte string with special PNG header
        Look for this header and return base64 encoded HTML for Jupyter display
    """
    import base64
    if blob[:8] == _png_header:
        b64_img_html = '<img src="data:image/png;base64,{}" />'
        b64_png = base64.b64encode(blob).decode('utf-8').replace('\n', '')
        return b64_img_html.format(b64_png)
    else:
        return blob


def pandas_df_to_html(df: DataFrame) -> Optional[str]:
    """Provide HTML formatting for pandas.DataFrame with rf_types.Tile in the columns.  """
    import pandas as pd
    # honor the existing options on display
    if not pd.get_option("display.notebook_repr_html"):
        return None

    default_max_colwidth = pd.get_option('display.max_colwidth')  # we'll try to politely put it back

    if len(df) == 0:
        return df._repr_html_()

    tile_cols = []
    geom_cols = []
    bytearray_cols = []
    for c in df.columns:
        if isinstance(df.iloc[0][c], pyrasterframes.rf_types.Tile):  # if the first is a Tile try formatting
            tile_cols.append(c)
        elif isinstance(df.iloc[0][c], BaseGeometry):  # if the first is a Geometry try formatting
            geom_cols.append(c)
        elif isinstance(df.iloc[0][c], bytearray):
            bytearray_cols.append(c)

    def _safe_tile_to_html(t):
        if isinstance(t, pyrasterframes.rf_types.Tile):
            return tile_to_html(t, fig_size=(2, 2))
        else:
            # handles case where objects in a column are not all Tile type
            return t.__repr__()

    def _safe_geom_to_html(g):
        if isinstance(g, BaseGeometry):
            wkt = g.wkt
            if len(wkt) > default_max_colwidth:
                return wkt[:default_max_colwidth - 3] + '...'
            else:
                return wkt
        else:
            return g.__repr__()

    def _safe_bytearray_to_html(b):
        if isinstance(b, bytearray):
            return binary_to_html(b)
        else:
            return b.__repr__()

    # dict keyed by column with custom rendering function
    formatter = {c: _safe_tile_to_html for c in tile_cols}
    formatter.update({c: _safe_geom_to_html for c in geom_cols})
    formatter.update({c: _safe_bytearray_to_html for c in bytearray_cols})

    # This is needed to avoid our tile being rendered as `<img src="only up to fifty char...`
    pd.set_option('display.max_colwidth', None)
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


def spark_df_to_markdown(df: DataFrame, num_rows: int = 5, truncate: bool = False) -> str:
    from pyrasterframes import RFContext
    return RFContext.active().call("_dfToMarkdown", df._jdf, num_rows, truncate)


def spark_df_to_html(df: DataFrame, num_rows: int = 5, truncate: bool = False) -> str:
    from pyrasterframes import RFContext
    return RFContext.active().call("_dfToHTML", df._jdf, num_rows, truncate)


def _folium_map_formatter(map) -> str:
    """ inputs a folium.Map object and returns html of rendered map """

    import base64
    html_source = map.get_root().render()
    b64_source = base64.b64encode(
        bytes(html_source.encode('utf-8'))
    ).decode('utf-8')

    source_blob = '<iframe src="data:text/html;charset=utf-8;base64,{}" allowfullscreen="" webkitallowfullscreen="" mozallowfullscreen="" style="position:relative;width:100%;height:500px"></iframe>'
    return source_blob.format(b64_source)


try:
    from IPython import get_ipython
    from IPython.display import display_png, display_markdown, display_html, display

    # modifications to currently running ipython session, if we are in one; these enable nicer visualization for Pandas
    if get_ipython() is not None:
        import pandas
        import pyspark.sql
        from pyrasterframes.rf_types import Tile

        ip = get_ipython()
        formatters = ip.display_formatter.formatters
        # Register custom formatters
        # PNG
        png_formatter = formatters['image/png']
        png_formatter.for_type(Tile, tile_to_png)
        # HTML
        html_formatter = formatters['text/html']
        html_formatter.for_type(pandas.DataFrame, pandas_df_to_html)
        html_formatter.for_type(pyspark.sql.DataFrame, spark_df_to_html)
        html_formatter.for_type(Tile, tile_to_html)

        # Markdown. These will likely only effect docs build.
        markdown_formatter = formatters['text/markdown']
        # Pandas doesn't have a markdown
        markdown_formatter.for_type(pandas.DataFrame, pandas_df_to_html)
        markdown_formatter.for_type(pyspark.sql.DataFrame, spark_df_to_markdown)
        # Running loose here by embedding tile as `img` tag.
        markdown_formatter.for_type(Tile, tile_to_html)

        try:
            # this block is to try to avoid making an install dep on folium but support if in the environment
            import folium

            markdown_formatter.for_type(folium.Map, _folium_map_formatter)
        except ImportError as e:
            pass

        Tile.show = plot_tile

        # noinspection PyTypeChecker
        def _display(df: pyspark.sql.DataFrame, num_rows: int = 5, truncate: bool = False,
                     mimetype: str = 'text/html') -> ():
            """
            Invoke IPython `display` with specific controls.
            :param num_rows: number of rows to render
            :param truncate: If `True`, shorten width of columns to no more than 40 characters
            :return: None
            """

            if "html" in mimetype:
                display_html(spark_df_to_html(df, num_rows, truncate), raw=True)
            else:
                display_markdown(spark_df_to_markdown(df, num_rows, truncate), raw=True)


        # Add enhanced display function
        pyspark.sql.DataFrame.display = _display

except ImportError as e:
    pass
