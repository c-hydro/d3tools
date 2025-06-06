import numpy as np
from matplotlib.colors import ListedColormap

from copy import deepcopy

from ..data import Dataset

def parse_colors(colors_definition: str|Dataset) -> tuple:
    '''
    Parse the color values from a text file.
    To create a text file, use the following format:
    -----------
    x1,r1,g1,b1,a1,label1
    x2,r2,g2,b2,a2,label2
    x3,r3,g3,b3,a3,label3
    x4,r4,g4,b4,a4,label4
    -------
    where:
    color r1,g1,b1,a1 is used for values below x1 (incl.);
    color r2,g2,b2,a2 is used for values between x1 (excl.) and x2 (incl.);
    ...and so on.
    x4 can be 'inf' to represent all values greater than x3.
    Any line that is not in this format will be ignored.

    labels are currently ignored.
    (this can be retrieved from QGIS by using the 'Export color map to text file' option in the layer styling panel.)

    It will return a tuple with two lists:
    - the first list contains the positions (i.e. [x1,x2,x3,x4])
    - and the second list contains the colors. (i.e. [(r1,g1,b1,a1),(r2,g2,b2,a2),(r3,g3,b3,a3),(r4,g4,b4,a4)])
    '''

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

    # Sort the color values by key
    sorted_colors = sorted(color_values.items(), key=lambda x: np.inf if x[0] == 'inf' else float(x[0]))

    # Separate the keys and values into separate lists
    positions, colors = zip(*sorted_colors)
    labels = [color_labels[p] for p in positions]
    return positions, colors, labels

def parse_line(line: str) -> tuple:
    '''
    Parse a single line from the text file.
    It will return a tuple with three values:
    - the position (x)
    - the color (r,g,b,a)
    - and the label
    '''
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

    # Normalize the colors to the range [0, 1]
    sorted_colors = np.array(sorted_colors, dtype=float) / 255

    # add a grey to colors for the nans
    if include_nan:
        sorted_colors = np.vstack((sorted_colors, nan_color))

    # Create the colormap
    cmap = ListedColormap(sorted_colors)

    return cmap

def keep_used_colors(unique_values:list[float], colors: list[tuple]) -> list[tuple]:
    '''
    Keep only the colors that are used in the list of unique values.
    We actually need to keep all the colors in the range of unique values
    '''

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

