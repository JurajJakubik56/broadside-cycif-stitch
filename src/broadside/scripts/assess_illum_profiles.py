import argparse
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import numpy.typing as npt
import tifffile
from bmd_perf.profiling import timed_ctx
from matplotlib import pyplot as plt, rcParams
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from matplotlib.gridspec import GridSpec, GridSpecFromSubplotSpec, SubplotSpec
from ome_types import from_xml
from scipy.signal import savgol_filter
from tifffile import TiffReader

from broadside.adjustments.hot_pixels import get_remove_hot_pixels_func_cyx
from broadside.utils.io import read_paths

rcParams["figure.dpi"] = 300
rcParams["font.family"] = "Arial"


@dataclass(frozen=True)
class TileQC:
    path: Path
    ipr: float  # inter-percentile range


def get_mean(path: Path, channel=0, mid_chunk_size=512) -> TileQC:
    with TiffReader(path) as tif:
        im = tif.pages[channel].asarray(maxworkers=1)
    assert im.ndim == 2
    h, w = im.shape
    im = im[
        h // 2 - mid_chunk_size // 2 : h // 2 + mid_chunk_size // 2,
        w // 2 - mid_chunk_size // 2 : w // 2 + mid_chunk_size // 2,
    ]
    p_lo, p_hi = np.percentile(im, (10, 90))
    return TileQC(path=path, ipr=(p_hi - p_lo).item())


def get_paths_with_low_contrast(paths: list[Path], n_tiles: int):
    with timed_ctx("get tile contrasts"):
        tiles_qc: list[TileQC] = [get_mean(p) for p in paths]
    tiles_by_ipr = sorted(tiles_qc, key=lambda t: t.ipr)
    return [tile.path for tile in tiles_by_ipr[:n_tiles]]


class Image:
    def __init__(
        self,
        path: Path,
        dark_dir: Path,
        flatfield: npt.NDArray,
        darkfield: npt.NDArray,
    ):
        with TiffReader(path) as reader:
            image = reader.asarray()
            ome = from_xml(reader.ome_metadata, parser="lxml")
            ts = ome.images[0].acquisition_date.timestamp()

        remove_hot_pixels = get_remove_hot_pixels_func_cyx(ts, dark_dir=dark_dir)
        self.raw: npt.NDArray = remove_hot_pixels(image)

        adjusted = self.raw - darkfield
        adjusted /= flatfield
        self.adj: npt.NDArray = adjusted

    def raw_smoothed(self, channel: int, row: int):
        return savgol_filter(self.raw[channel, row], window_length=80, polyorder=1)

    def adj_smoothed(self, channel: int, row: int):
        return savgol_filter(self.adj[channel, row], window_length=80, polyorder=1)


def get_channel_props(path: Path):
    with TiffReader(path) as reader:
        ome = from_xml(reader.ome_metadata, parser="lxml")
        channels = ome.images[0].pixels.channels
    props = [
        dict(i=i, biomarker=channel.name, fluorophore=channel.fluor)
        for i, channel in enumerate(channels)
    ]
    return props


def plot(
    round_name: str,
    tile_paths: list[Path],
    flatfield_path: Path,
    darkfield_path: Path,
    dark_dir: Path,
    dst: Path,
):
    flatfield: npt.NDArray = tifffile.imread(flatfield_path, maxworkers=1)
    darkfield: npt.NDArray = tifffile.imread(darkfield_path, maxworkers=1)

    channel_props = get_channel_props(tile_paths[0])
    channel_names = [f'{p["biomarker"]}\n({p["fluorophore"]})' for p in channel_props]

    images = [
        Image(
            path=tile_path,
            dark_dir=dark_dir,
            flatfield=flatfield,
            darkfield=darkfield,
        )
        for tile_path in tile_paths
    ]
    exemplar = images[0]

    n_tiles = len(tile_paths)
    n_channels = exemplar.raw.shape[0]

    h = exemplar.raw.shape[1]
    i_ys = np.linspace(0, h, 10, endpoint=False, dtype=int)

    cmap = plt.get_cmap("tab10")

    fig: Figure = plt.figure(figsize=(2 * n_channels, 1.25 * n_tiles))
    outer_grid: GridSpec = fig.add_gridspec(nrows=n_tiles, ncols=n_channels)
    for i_tile in range(n_tiles):
        for channel in range(n_channels):
            ss: SubplotSpec = outer_grid[i_tile, channel]
            inner_grid: GridSpecFromSubplotSpec = ss.subgridspec(nrows=1, ncols=2)
            axes: npt.NDArray[Axes] = inner_grid.subplots(sharex=True)

            # raw image
            ax_raw: Axes = axes[0]
            if ss.is_first_row():
                ax_raw.set_title("Raw")
            if ss.is_first_col():
                ax_raw.yaxis.set_label_text(tile_paths[i_tile].stem)
            ax_raw.xaxis.set_ticks([])
            ax_raw.xaxis.set_ticklabels([])
            ax_raw.yaxis.set_ticks([])
            ax_raw.yaxis.set_ticklabels([])

            datas = []
            for i, i_y in enumerate(i_ys):
                data = images[i_tile].raw_smoothed(channel=channel, row=i_y) - i
                datas.append(data)
                ax_raw.plot(data, c=cmap(i), alpha=0.4, linewidth=1)

            mid_val = np.median(datas)
            min_val = np.percentile(datas, 0.5)
            delta = (mid_val - min_val) * 1.2
            ax_raw.set_ylim(mid_val - delta, mid_val + delta)

            # adjusted image
            ax_adj: Axes = axes[1]
            if ss.is_first_row():
                ax_adj.set_title("Adjusted")
            ax_adj.xaxis.set_ticklabels([])
            ax_adj.yaxis.set_ticklabels([])
            ax_adj.xaxis.set_ticks([])
            ax_adj.yaxis.set_ticks([])

            datas = []
            for i, i_y in enumerate(i_ys):
                data = images[i_tile].adj_smoothed(channel=channel, row=i_y) - i
                datas.append(data)
                ax_adj.plot(data, c=cmap(i % 10), alpha=0.4, linewidth=1)

            mid_val = np.median(datas)
            min_val = np.percentile(datas, 0.5)
            delta = (mid_val - min_val) * 1.2
            ax_adj.set_ylim(mid_val - delta, mid_val + delta)

            # container axes for grouped labels
            if ss.is_first_row():
                cont_grid: GridSpecFromSubplotSpec = ss.subgridspec(nrows=1, ncols=1)
                cont_ax: Axes = cont_grid.subplots()
                cont_ax.set_facecolor((0, 0, 0, 0))  # clear
                cont_ax.set_title(channel_names[channel], pad=30)
                for side in ["left", "right", "top", "bottom"]:
                    cont_ax.spines[side].set_visible(False)
                cont_ax.xaxis.set_ticklabels([])
                cont_ax.yaxis.set_ticklabels([])
                cont_ax.xaxis.set_ticks([])
                cont_ax.yaxis.set_ticks([])

    fig.suptitle(f"Illum. corrections for round {round_name}")
    fig.supylabel("Pixel Intensity")
    fig.supxlabel("X")

    fig.tight_layout()
    fig.savefig(dst)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--round-name", type=str, required=True)
    parser.add_argument("--tiles-path", type=str, required=True)
    parser.add_argument("--n-tiles", type=int, required=True)
    parser.add_argument("--flatfield-path", type=str, required=True)
    parser.add_argument("--darkfield-path", type=str, required=True)
    parser.add_argument("--dark-dir", type=str, required=True)
    parser.add_argument("--dst", type=str, required=True)

    args = parser.parse_args()
    paths_low_contrast = get_paths_with_low_contrast(
        paths=read_paths(Path(args.tiles_path)), n_tiles=args.n_tiles
    )

    plot(
        round_name=args.round_name,
        tile_paths=paths_low_contrast,
        flatfield_path=Path(args.flatfield_path),
        darkfield_path=Path(args.darkfield_path),
        dark_dir=Path(args.dark_dir),
        dst=Path(args.dst),
    )
