"""Helpers for parsing and validating class-break color definitions.

The functions in this module read textual color maps in the QGIS-style format
used by the thumbnail renderer and convert them into the ordered breakpoints and
RGBA tuples expected by Matplotlib.
"""

import numpy as np
from matplotlib.colors import ListedColormap

from copy import deepcopy

from ..data import Dataset

def parse_colors(colors_definition: str|Dataset) -> tuple:
    """Parse a color-definition file or dataset into breakpoint, color, and label values.

    The accepted text format is one row per class break:

        x1,r1,g1,b1,a1,label1
        x2,r2,g2,b2,a2,label2
        ...

    Each row defines a numeric threshold and an RGBA tuple. The final row may
    use ``inf`` to represent all values above the previous breakpoint. Lines that
    do not match the expected format are ignored. Labels are retained for legend
    output but are not otherwise used by the renderer.

    Args:
        colors_definition: A path to the color map text file or a ``Dataset``
            whose ``get_data()`` output yields the same text lines.

    Returns:
        tuple: A three-item tuple containing the sorted break positions, the
        corresponding RGBA color tuples, and the associated labels.

    Raises:
        ValueError: If no valid color definitions are found in the input.
    """

    if isinstance(colors_definition, str):
        txt_file = colors_definition
        with open(txt_file, 'r') as f:
            lines = f.readlines()
    else:
        lines = colors_definition.get_data()

    # skip all the lines at that have invalid format

    color_values = {}
    color_labels = {}   
    for line in lines:
        try:
            value, color, label = parse_line(line)
            color_values[value] = color
            color_labels[value] = label
        except ValueError:
            pass

    if not color_values:
        raise ValueError("No valid color definitions were found.")

    # Sort the color values by key
    sorted_colors = sorted(color_values.items(), key=lambda x: np.inf if x[0] == 'inf' else float(x[0]))

    # Separate the keys and values into separate lists
    positions, colors = zip(*sorted_colors)
    labels = [color_labels[p] for p in positions]
    return positions, colors, labels

def parse_line(line: str) -> tuple:
    """Parse one row from a QGIS-style color map.

    Args:
        line: A comma-separated row with the format
            ``position,r,g,b,a,label``. The label may itself contain commas and
            is reconstructed from the remaining text after the first five color
            fields.

    Returns:
        tuple: ``(position, color_values, label)`` where ``position`` is the
        numeric breakpoint or ``"inf"`` and ``color_values`` is a list of four
        integers in the range 0-255.

    Raises:
        ValueError: If the row is malformed or the numeric breakpoint or RGBA
            values are invalid.
    """
    parts = line.strip().split(',')
    if len(parts) < 6:
        raise ValueError(f'Invalid line format {line}. Expected 6 values separated by commas')

    position, color, label = parts[0], parts[1:5], '.'.join(parts[5:])

    # ensure position is a valid number or 'inf'
    if position != 'inf':
        try:
            float(position)
        except ValueError:
            raise ValueError('Invalid position value. Expected a number or "inf"')
        
    # ensure color values are valid integers between 0 and 255
    for c in color:
        try:
            c = int(c)
            if c < 0 or c > 255:
                raise ValueError('Invalid color value. Expected an integer between 0 and 255')
        except ValueError:
            raise ValueError('Invalid color value. Expected an integer between 0 and 255')
        
    return position, color, label

def create_colormap(sorted_colors, nan_color: list[float] = [0.5, 0.5, 0.5, 1.0], include_nan = True) -> ListedColormap:
    """Build a Matplotlib colormap from normalized RGBA values.

    Args:
        sorted_colors: RGBA tuples in 0-255 channel space.
        nan_color: RGBA values used for missing-data entries when ``include_nan``
            is true.
        include_nan: If true, append ``nan_color`` as the final colormap entry.

    Returns:
        matplotlib.colors.ListedColormap: A colormap with 8-bit color channels
        normalized to the 0-1 range expected by Matplotlib.
    """

    # Normalize the colors to the range [0, 1]
    sorted_colors = np.array(sorted_colors, dtype=float) / 255

    # add a grey to colors for the nans
    if include_nan:
        sorted_colors = np.vstack((sorted_colors, nan_color))

    # Create the colormap
    cmap = ListedColormap(sorted_colors)

    return cmap

def keep_used_colors(unique_values:list[float], colors: list[tuple]) -> list[tuple]:
    """Return the subset of colors spanning the observed value range.

    The function does not drop colors by exact value lookup; instead it keeps the
    contiguous slice of the palette from the minimum unique value to the maximum
    unique value, inclusive.

    Args:
        unique_values: Numeric class indices that appear in the rendered data.
        colors: Ordered palette entries corresponding to the full class range.

    Returns:
        list[tuple]: The palette slice needed for the observed value range.
    """

    #col_list = list(deepcopy(colors))
    # to_remove = []
    # for i in range(len(col_list)):
    #     if i not in unique_values:
    #         to_remove.append(i)

    # # Sort to_remove in descending order
    # to_remove.sort(reverse=True)

    # for i in to_remove:
    #     col_list.pop(i)

    return colors[min(unique_values):max(unique_values)+1]

