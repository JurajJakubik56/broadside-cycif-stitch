import argparse
import json
from pathlib import Path
from typing import Optional, Callable

from joblib import delayed, Parallel
from ome_types import OME, from_xml
from ome_types.model.simple_types import UnitsLength
from tifffile import tiffcomment, TiffReader

from broadside.utils.geoms import Point2D, get_center_of_points
from broadside.utils.io import read_paths
from broadside.utils.units import ureg


def _get_ome_position(path: Path) -> Point2D:
    with TiffReader(path) as reader:
        ome = from_xml(reader.ome_metadata, parser="lxml")
        ref_plane = ome.images[0].pixels.planes[0]

    pos_x = ref_plane.position_x * getattr(ureg, ref_plane.position_x_unit.name.lower())
    pos_x = pos_x.to(ureg.micrometer).magnitude

    pos_y = ref_plane.position_y * getattr(ureg, ref_plane.position_y_unit.name.lower())
    pos_y = pos_y.to(ureg.micrometer).magnitude

    return Point2D(x=pos_x, y=pos_y)


def get_scene_center(
    tile_paths_by_round: dict[str, list[Path]], round_names: list[str]
) -> Point2D:
    # we use the first round since that has the least tissue loss
    tile_paths = tile_paths_by_round[round_names[0]]
    positions = Parallel(n_jobs=2, backend="threading")(
        delayed(_get_ome_position)(path) for path in tile_paths
    )
    px = [p.x for p in positions]
    py = [p.y for p in positions]
    center = get_center_of_points(px, py)
    center_x, center_y = center.y, center.x
    # we invert y because the slide is rotated 90 degrees CCW, which can be done per
    # tile by rot90, keeping the origin at the upper left, but needs to be X,Y->Y,-X
    # for single points
    return Point2D(x=center_x, y=-center_y)


def unique_list(objs: list, key: Callable):
    keys = []
    s = []
    for obj in objs:
        k = key(obj)
        if k not in keys:
            s.append(obj)
            keys.append(k)

    return s


def make_ome(
    *,
    ashlar_first_channel_path: Path,
    ome_image_name: str,
    tile_paths_by_round: dict[str, list[Path]],
    round_names: list[str],
):
    # get full size from ashlar
    with TiffReader(ashlar_first_channel_path) as ashlar_reader:
        shape = ashlar_reader.pages[0].shape
        assert len(shape) == 2
        size_y, size_x = shape

    centroid = get_scene_center(tile_paths_by_round, round_names)

    base_ome: Optional[OME] = None
    all_filters = []
    all_channels = []
    all_planes = []
    the_c = 0
    the_ch = 0
    for round_name in round_names:
        last_path = tile_paths_by_round[round_name][-1]

        with TiffReader(last_path) as reader:
            ome: OME = from_xml(reader.ome_metadata, parser="lxml").copy()

        all_filters.extend(ome.instruments[0].filters)

        assert len(ome.images) == 1
        pixels = ome.images[0].pixels

        # save ome of last round; this will be the exemplar ome
        if round_name == round_names[-1]:
            base_ome = ome

        for plane in pixels.planes:
            plane.the_c = the_c
            plane.position_x = centroid.x
            plane.position_x_unit = UnitsLength.MICROMETER
            plane.position_y = centroid.y
            plane.position_y_unit = UnitsLength.MICROMETER
            the_c += 1
        all_planes.extend(pixels.planes)

        for i_ch, channel in enumerate(pixels.channels):
            channel.id = f"Channel:{the_ch}"
            channel.name = json.dumps(
                dict(
                    cycle=round_name,
                    channel=i_ch,
                    biomarker=channel.name,
                    fluorophore=channel.fluor,
                )
            )
            the_ch += 1
        all_channels.extend(pixels.channels)

    all_filters = unique_list(all_filters, lambda f: f.id)

    assert base_ome is not None
    base_ome.instruments[0].filters = all_filters

    base_image = base_ome.images[0]
    base_image.id = "Image:0"
    base_image.name = ome_image_name
    base_image.pixels.id = "Pixels:0"
    base_image.pixels.channels = all_channels
    base_image.pixels.planes = all_planes
    base_image.pixels.size_x = size_x
    base_image.pixels.size_y = size_y
    base_image.pixels.size_c = len(all_channels)
    base_image.pixels.tiff_data_blocks[0].plane_count = len(all_channels)

    return base_ome


def read_tiles_by_round(path: Path) -> tuple[list[str], dict[str, list[Path]]]:
    newline = "\n"
    col_sep = "\t"

    with path.open("r") as file:
        contents = file.read()
    lines = [l.strip() for l in contents.split(newline)[1:]]
    lines = [l.split(col_sep) for l in lines]
    round_names = [l[0].strip() for l in lines]
    tile_paths_by_round = {l[0]: read_paths(Path(l[1].strip())) for l in lines}
    return round_names, tile_paths_by_round


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--ashlar-first-channel", type=str, required=True)
    parser.add_argument("--stacks-path", type=str, required=True)
    parser.add_argument("--tiles-path", type=str, required=True)
    # parser.add_argument("--ome-tiff-path", type=str, required=True)
    parser.add_argument("--ome-xml-path", type=str, required=True)
    parser.add_argument("--ome-image-name", type=str, required=True)

    args = parser.parse_args()

    round_names, tile_paths_by_round = read_tiles_by_round(Path(args.tiles_path))

    ome = make_ome(
        ashlar_first_channel_path=Path(args.ashlar_first_channel),
        ome_image_name=args.ome_image_name,
        tile_paths_by_round=tile_paths_by_round,
        round_names=round_names,
    )
    ome_xml = ome.to_xml()
    # ome_tiff_path = Path(args.ome_tiff_path)
    # tiffcomment(ome_tiff_path, ome_xml.encode())

    ome_xml_path = Path(args.ome_xml_path)
    with ome_xml_path.open("w") as file:
        file.write(ome_xml)
