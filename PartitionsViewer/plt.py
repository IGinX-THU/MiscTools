import functools

from distinctipy import distinctipy
from PIL import ImageFont, Image, ImageDraw

from entity import KeyRange, ColumnRange

WIDEN_PER_FRAGMENT = 150
HEIGHT_PER_FRAGMENT = 150

TEXT_WIDEN_DEVIATION = 5
TEXT_HEIGHT_DEVIATION = 5

BORDER = 50

RIGHT_EXTRA_SPACE = 50

HEIGHT_PER_STORAGE = 25


def plot(storage_engines, storage_units, fragments, target):
    column_bounds = set()
    key_bounds = set()
    for fragment in fragments:
        column_bounds.add(fragment.column_range.start)
        column_bounds.add(fragment.column_range.end)
        key_bounds.add(fragment.key_range.start)
        key_bounds.add(fragment.key_range.end)

    column_bounds_map = get_column_bounds_map(column_bounds)

    key_bounds_map = get_key_bounds_map(key_bounds)

    def get_fragment_storage_engine(fragment):
        unit_id = fragment.storage_unit_id
        storage_id = None
        for storage_unit in storage_units:
            if storage_unit.id != unit_id:
                continue
            storage_id = storage_unit.storage_id
            break

        if storage_id is None:
            raise ValueError("invalid storage unit id " + str(unit_id))
        for storage_engine in storage_engines:
            if storage_engine.id == storage_id:
                return storage_engine
        raise ValueError("invalid storage engine id " + str(storage_id))

    colors = distinctipy.get_colors(len(storage_engines))
    colors = [distinctipy.get_hex(color) for color in colors]

    def calculate_boundary():
        w, h = BORDER * 2 + WIDEN_PER_FRAGMENT * (len(key_bounds_map) - 1) + RIGHT_EXTRA_SPACE, BORDER * 3 + HEIGHT_PER_FRAGMENT * (len(column_bounds_map) - 1) + len(storage_engines) * HEIGHT_PER_STORAGE
        if w < 320:
            w = 320
        return w, h

    def get_coordinate_for_fragment(key, column):
        return BORDER + key_bounds_map[key] * WIDEN_PER_FRAGMENT, BORDER + column_bounds_map[column] * HEIGHT_PER_FRAGMENT

    def get_coordinate_for_text(key, column):
        index_x, index_y = get_coordinate_for_fragment(key, column)
        return index_x + TEXT_WIDEN_DEVIATION, index_y + TEXT_HEIGHT_DEVIATION

    def get_coordinate_for_storage_engine(index):
        return BORDER, BORDER * 2 + HEIGHT_PER_FRAGMENT * (len(column_bounds_map) - 1) + index * HEIGHT_PER_STORAGE

    def get_text_for_coordinate(key, column):
        if column == ColumnRange.UNBOUNDED_FROM or column == ColumnRange.UNBOUNDED_TO:
            column = "null"
        if key == 0:
            key = "min"
        if key == KeyRange.MAX_KEY:
            key = "max"
        return str(column) + ", " + str(key)

    image = Image.new('RGB', calculate_boundary(), 'white')

    text_font = ImageFont.truetype("alibaba.ttf", 10)
    storage_font = ImageFont.truetype("alibaba.ttf", 10)
    draw = ImageDraw.Draw(image)

    for fragment in fragments:
        start_key, end_key = fragment.key_range.start, fragment.key_range.end
        start_column, end_column = fragment.column_range.start, fragment.column_range.end
        from_x, from_y = get_coordinate_for_fragment(start_key, start_column)
        to_x, to_y = get_coordinate_for_fragment(end_key, end_column)
        draw.rectangle((from_x, from_y, to_x, to_y), fill=colors[get_fragment_storage_engine(fragment).id], outline='black')
        draw.text(get_coordinate_for_text(start_key, start_column), font=text_font, text=get_text_for_coordinate(start_key, start_column), fill='black')
        draw.text(get_coordinate_for_text(end_key, end_column), font=text_font, text=get_text_for_coordinate(end_key, end_column), fill='black', spacing=5)

    for i in range(len(storage_engines)):
        storage_engine = storage_engines[i]
        draw.text(get_coordinate_for_storage_engine(i), font=storage_font, text=str(storage_engine), fill=colors[storage_engine.id])

    image.save(target)


def compare_column(x, y):
    if x == y:
        return 0
    if x == ColumnRange.UNBOUNDED_FROM or y == ColumnRange.UNBOUNDED_TO:
        return -1
    if x == ColumnRange.UNBOUNDED_TO or y == ColumnRange.UNBOUNDED_FROM:
        return 1
    return  -1 if x < y else 1

def get_column_bounds_map(column_bounds):
    column_bounds = list(column_bounds)
    column_bounds.sort(key=functools.cmp_to_key(compare_column))
    column_bounds_map = {}
    for i in range(len(column_bounds)):
        column_bounds_map[column_bounds[i]] = i
    return column_bounds_map


def get_key_bounds_map(key_bounds):
    key_bounds = list(key_bounds)
    key_bounds.sort()
    key_bounds_map = {}
    for i in range(len(key_bounds)):
        key_bounds_map[key_bounds[i]] = i
    return key_bounds_map