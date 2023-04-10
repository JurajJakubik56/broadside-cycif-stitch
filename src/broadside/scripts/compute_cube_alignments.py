import argparse
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import dask
import imageio
import numpy as np
import numpy.typing as npt
import pandas as pd
from PIL import Image, ImageFont, ImageDraw
from dask import delayed
from dateutil import tz
from matplotlib import pyplot as plt, rcParams
from matplotlib.axes import Axes
from matplotlib.figure import Figure
from ome_types import from_xml
from pint import Quantity
from scipy.ndimage import gaussian_laplace
from skimage import img_as_float, img_as_ubyte
from skimage.exposure import rescale_intensity
from skimage.registration import phase_cross_correlation
from tifffile import TiffReader

from broadside.adjustments.alignment import scale_image, shift_image
from broadside.config import re_filter, re_wavelength
from broadside.utils.geoms import Point2D
from broadside.utils.parallel import dask_session
from broadside.utils.units import ureg


# PLOTTING =========================================================================== #


def _unix_to_datetime(ts: float | int) -> datetime:
    dt = datetime.fromtimestamp(ts, tz=tz.tzutc())
    dt = dt.astimezone(tz=tz.tzlocal())
    return dt


def _plot_ax_z(ax: Axes, df: pd.DataFrame):
    df = df.copy()
    cubes = df["cube"].unique()

    # z heights only make sense in a relative sense, so subtract the first value
    df["z_um"] -= df["z_um"][0]

    for cube in cubes:
        df_cube = df[df["cube"] == cube]
        ax.plot(df_cube["wavelength"], df_cube["z_um"], label=cube)

    ax.yaxis.set_label_text("Z height (µm)")
    ax.xaxis.set_label_text("Wavelength (nm)")


def _plot_ax_scales(ax: Axes, df: pd.DataFrame):
    cubes = df["cube"].unique()

    for cube in cubes:
        df_cube = df[df["cube"] == cube]
        ax.plot(df_cube["wavelength"], df_cube["scale"], label=cube)

    ax.yaxis.set_label_text("Factor")
    ax.xaxis.set_label_text("Wavelength (nm)")
    ax.set_title("Scaling")
    ax.legend()


def _plot_ax_shifts_after_scaling(ax: Axes, df: pd.DataFrame):
    cubes = df["cube"].unique()

    cmap = plt.get_cmap("tab10")

    for i, cube in enumerate(cubes):
        df_cube = df[df["cube"] == cube]
        ax.plot(df_cube["shift_x"], df_cube["shift_y"], label=cube, c=cmap(i))
        ax.text(
            df_cube["shift_x"].iloc[0],
            df_cube["shift_y"].iloc[0],
            s=f'{df_cube["wavelength"].iloc[0]}nm',
            fontdict=dict(size=8, color=cmap(i), alpha=0.5),
        )

    ax.set_aspect("equal")
    ax.xaxis.set_label_text("dx (px)")
    ax.yaxis.set_label_text("dy (px)")
    ax.set_title("Shift after Scaling")


def _plot_cube_alignments(csv_path: Path, dst: Path, timestamp: int):
    rcParams["figure.dpi"] = 300
    rcParams["font.family"] = "Arial"

    dt = _unix_to_datetime(timestamp)
    df = pd.read_csv(csv_path)

    fig, axes = plt.subplots(ncols=3, nrows=1, figsize=(10, 4))
    fig: Figure
    _plot_ax_z(axes[0], df)
    _plot_ax_scales(axes[1], df)
    _plot_ax_shifts_after_scaling(axes[2], df)

    dt_as_str = dt.isoformat(timespec="seconds")
    fig.suptitle(f"Chromatic Aberration\nObtained {dt_as_str}")
    fig.tight_layout()
    fig.savefig(dst)


# PILLOW ============================================================================= #


def _add_text_bbox(
    *,
    draw: ImageDraw,
    position: tuple[float, float],
    text: str,
    text_color,
    bbox_color,
    font: ImageFont,
    border: int,
):
    left, top, right, bottom = draw.textbbox(position, text, font=font)
    draw.rectangle(
        (left - border, top - border, right + border, bottom + border), fill=bbox_color
    )
    draw.text(position, text, font=font, fill=text_color)


# COMPUTING ========================================================================== #


def compute_large_shift(
    im_ref: npt.NDArray,
    im_mov: npt.NDArray,
    *,
    shift_approx: Point2D,
    upsample_factor: int,
) -> float:
    assert isinstance(shift_approx.x, Quantity)
    assert isinstance(shift_approx.y, Quantity)

    x = int(shift_approx.x.to(ureg.pixel).magnitude)
    y = int(shift_approx.y.to(ureg.pixel).magnitude)

    if x > 0:
        ref_x_slice = slice(x, None)
        mov_x_slice = slice(None, -x)
    elif x < 0:
        ref_x_slice = slice(None, -x)
        mov_x_slice = slice(x, None)
    else:
        ref_x_slice = slice(None)
        mov_x_slice = slice(None)

    if y > 0:
        ref_y_slice = slice(y, None)
        mov_y_slice = slice(None, -y)
    elif y < 0:
        ref_y_slice = slice(None, -y)
        mov_y_slice = slice(y, None)
    else:
        ref_y_slice = slice(None)
        mov_y_slice = slice(None)

    im_ref = im_ref[ref_y_slice, ref_x_slice]
    im_mov = im_mov[mov_y_slice, mov_x_slice]

    shift = phase_cross_correlation(
        im_ref,
        im_mov,
        upsample_factor=upsample_factor,
        return_error=False,
    )
    return math.hypot(
        im_ref.shape[0] - y + shift[0],
        im_ref.shape[1] - x + shift[1],
    )


@dataclass
class Channel:
    orig: npt.NDArray
    z: float
    sharpened: npt.NDArray | None = None

    scale: float | None = None
    scaled: npt.NDArray | None = None

    shift: Point2D | None = None
    cumul_shift: Point2D | None = None
    shifted: npt.NDArray | None = None

    corrected: npt.NDArray | None = None


@dataclass(frozen=True)
class Stack:
    channels: dict[str, dict[int, Channel]]
    pos: Point2D

    @property
    def index(self):
        for cube, cube_channels in self.channels.items():
            for wavelength in cube_channels.keys():
                yield cube, wavelength

    def __getitem__(self, item: tuple[str, int]):
        cube, wavelength = item
        return self.channels[cube][wavelength]

    @classmethod
    def from_path(cls, path: Path):
        with TiffReader(path) as reader:
            array = reader.asarray()
            ome = from_xml(reader.ome_metadata, parser="lxml")

        pixels = ome.images[0].pixels
        plane0 = pixels.planes[0]

        x = plane0.position_x
        x_unit = plane0.position_x_unit.name.lower()
        x *= getattr(ureg, x_unit)

        x_mpp = pixels.physical_size_x
        x_mpp_unit = pixels.physical_size_x_unit.name.lower()
        x_mpp *= getattr(ureg, x_mpp_unit) / ureg.pixel

        y = plane0.position_y
        y_unit = plane0.position_y_unit.name.lower()
        y *= getattr(ureg, y_unit)

        y_mpp = pixels.physical_size_y
        y_mpp_unit = pixels.physical_size_y_unit.name.lower()
        y_mpp *= getattr(ureg, y_mpp_unit) / ureg.pixel

        # arrays
        array = img_as_float(array)
        channels = {}
        for i_ch, (channel, plane) in enumerate(zip(pixels.channels, pixels.planes)):
            em_refs = channel.light_path.emission_filter_ref
            cube = re_filter.match(em_refs[0].id)["cube"]
            wavelength = int(re_wavelength.match(em_refs[1].id)["wavelength"])

            z = plane.position_z
            z_unit = plane.position_z_unit.name.lower()
            z *= getattr(ureg, z_unit)

            channel = Channel(orig=array[i_ch], z=z)
            try:
                channels[cube][wavelength] = channel
            except KeyError:
                channels[cube] = {wavelength: channel}

        return cls(channels=channels, pos=Point2D(x=x / x_mpp, y=y / y_mpp))

    def sharpen(self, sigma: float):
        def _sharpen(arr: npt.NDArray, sigma: float):
            arr = rescale_intensity(arr)
            output = gaussian_laplace(arr, sigma)
            return output

        # with timed_ctx("sharpened"):
        #     for index in self.index:
        #         self[index].sharpened = _sharpen(self[index].orig, sigma=sigma)

        delayeds = [
            delayed(_sharpen)(self[index].orig, sigma=sigma) for index in self.index
        ]
        results = dask.compute(*delayeds)
        for i, index in enumerate(self.index):
            self[index].sharpened = results[i]

    def scale(self):
        # with timed_ctx("scale"):
        #     for index in self.index:
        #         self[index].scaled = scale_image(
        #             self[index].sharpened, self[index].scale
        #         )

        delayeds = [
            delayed(scale_image)(self[index].sharpened, self[index].scale)
            for index in self.index
        ]
        results = dask.compute(*delayeds)
        for i, index in enumerate(self.index):
            self[index].scaled = results[i]

    def compute_shifts(self, upsample_factor=100):
        # for cube, channels in self.channels.items():
        #     wavelengths = sorted(list(channels.keys()))
        #     min_wavelength = wavelengths[0]
        #     for wavelength in wavelengths:
        #         if wavelength == min_wavelength:
        #             im_ref = self[("DAPI", wavelength)].scaled
        #         else:
        #             im_ref = self[(cube, wavelength - 10)].scaled
        #         im_mov = self[(cube, wavelength)].scaled
        #
        #         shift = phase_cross_correlation(
        #             im_ref,
        #             im_mov,
        #             upsample_factor=upsample_factor,
        #             return_error=False,
        #         )
        #         self[(cube, wavelength)].shift = Point2D(y=shift[0], x=shift[1])

        delayeds = []
        for cube, channels in self.channels.items():
            wavelengths = sorted(list(channels.keys()))
            min_wavelength = wavelengths[0]
            for wavelength in wavelengths:
                if wavelength == min_wavelength:
                    im_ref = self[("DAPI", wavelength)].scaled
                else:
                    im_ref = self[(cube, wavelength - 10)].scaled
                im_mov = self[(cube, wavelength)].scaled

                delayeds.append(
                    delayed(phase_cross_correlation)(
                        im_ref,
                        im_mov,
                        upsample_factor=upsample_factor,
                        return_error=False,
                    )
                )

        results = dask.compute(*delayeds)
        index = 0
        for cube, channels in self.channels.items():
            wavelengths = sorted(list(channels.keys()))
            for wavelength in wavelengths:
                shift = results[index]
                self[(cube, wavelength)].shift = Point2D(y=shift[0], x=shift[1])
                index += 1

        for cube, channels in self.channels.items():
            wavelengths = sorted(list(channels.keys()))
            min_wavelength = wavelengths[0]
            if cube == "DAPI":
                dapi_cumul_shift = Point2D(x=0, y=0)
            else:
                dapi_shifts = [
                    self[("DAPI", w)].shift for w in wavelengths if w <= min_wavelength
                ]
                dapi_cumul_shift = Point2D(
                    x=sum([s.x for s in dapi_shifts]),
                    y=sum([s.y for s in dapi_shifts]),
                )

            for wavelength in wavelengths:
                wavelengths_cube = [
                    w for w in wavelengths if min_wavelength <= w <= wavelength
                ]
                cube_shifts = [self[(cube, w)].shift for w in wavelengths_cube]
                cube_cumul_shift = Point2D(
                    x=sum([s.x for s in cube_shifts]),
                    y=sum([s.y for s in cube_shifts]),
                )
                self[(cube, wavelength)].cumul_shift = (
                    cube_cumul_shift + dapi_cumul_shift
                )

    def shift(self):
        # with timed_ctx("shift"):
        #     for index in self.index:
        #         self[index].shifted = shift_image(
        #             self[index].scaled, self[index].cumul_shift
        #         )

        delayeds = [
            delayed(shift_image)(self[index].scaled, self[index].cumul_shift)
            for index in self.index
        ]
        results = dask.compute(*delayeds)
        for i, index in enumerate(self.index):
            self[index].shifted = results[i]

    def apply(self):
        def _apply(im: npt.NDArray, scale: float, shift: Point2D) -> npt.NDArray:
            im = scale_image(im, scale)
            im = shift_image(im, shift)
            return im

        # for index in self.index:
        #     self[index].corrected = _apply(
        #         self[index].orig, self[index].scale, self[index].cumul_shift
        #     )

        delayeds = [
            delayed(_apply)(
                self[index].orig, self[index].scale, self[index].cumul_shift
            )
            for index in self.index
        ]
        results = dask.compute(*delayeds)
        for i, index in enumerate(self.index):
            self[index].corrected = results[i]


@dataclass(frozen=True)
class StackPair:
    ref: Stack
    mov: Stack

    def sharpen(self, sigma: float):
        self.ref.sharpen(sigma=sigma)
        self.mov.sharpen(sigma=sigma)

    def compute_scales(self, upsample_factor: int):
        large_shifts = {}
        # for index in self.ref.index:
        #     large_shifts[index] = compute_large_shift(
        #         self.ref[index].sharpened,
        #         self.mov[index].sharpened,
        #         shift_approx=self.mov.pos - self.ref.pos,
        #         upsample_factor=upsample_factor,
        #     )

        delayeds = [
            delayed(compute_large_shift)(
                self.ref[index].sharpened,
                self.mov[index].sharpened,
                shift_approx=self.mov.pos - self.ref.pos,
                upsample_factor=upsample_factor,
            )
            for index in self.ref.index
        ]
        results = dask.compute(*delayeds)
        for i, index in enumerate(self.ref.index):
            large_shifts[index] = results[i]

        ref_shift = large_shifts[("DAPI", 460)]
        for index in self.ref.index:
            scale = large_shifts[index] / ref_shift
            scale = (scale - 1) * 2 + 1
            self.ref[index].scale = scale
            self.mov[index].scale = scale

    def scale(self):
        self.ref.scale()
        self.mov.scale()

    def compute_shifts(self, upsample_factor=100):
        self.ref.compute_shifts(upsample_factor)
        self.mov.compute_shifts(upsample_factor)

    def shift(self):
        self.ref.shift()
        self.mov.shift()

    def save_as_csv(self, dst: Path):
        records = []
        for index in self.ref.index:
            scale = (self.ref[index].scale + self.mov[index].scale) / 2
            cumul_shift = (
                self.ref[index].cumul_shift + self.mov[index].cumul_shift
            ) / 2
            z = (self.ref[index].z + self.mov[index].z) / 2
            z = z.to(ureg.micrometer).magnitude

            cube, wavelength = index
            records.append(
                dict(
                    cube=cube,
                    wavelength=wavelength,
                    scale=scale,
                    shift_x=cumul_shift.x,
                    shift_y=cumul_shift.y,
                    z_um=z,
                )
            )

        df = pd.DataFrame.from_records(records)
        df.to_csv(dst, index=False)

    def apply(self):
        self.ref.apply()
        self.mov.apply()

    def save_as_gif(self, dst: Path):
        big_font = ImageFont.truetype("Arial.ttf", size=36)
        small_font = ImageFont.truetype("Arial.ttf", size=18)

        frames = []
        for index in self.ref.index:
            orig = self.ref[index].orig
            orig = orig[-500:, -250:]
            orig = rescale_intensity(orig)
            orig = img_as_ubyte(orig)

            # corrected = self.ref[index].orig
            corrected = self.ref[index].corrected
            corrected = corrected[-500:, -250:]
            corrected = rescale_intensity(corrected)
            corrected = img_as_ubyte(corrected)

            frame = np.concatenate([orig, corrected], axis=1)
            pil_frame = Image.fromarray(frame, mode="L")
            draw = ImageDraw.Draw(pil_frame)

            border = 5

            _add_text_bbox(
                draw=draw,
                position=(10 + border, border),
                text="Original",
                text_color="black",
                bbox_color="white",
                font=big_font,
                border=border,
            )

            _add_text_bbox(
                draw=draw,
                position=(10 + 250 + border, border),
                text="Corrected",
                text_color="black",
                bbox_color="white",
                font=big_font,
                border=border,
            )

            cube, wavelength = index
            _add_text_bbox(
                draw=draw,
                position=(10 + border, 430 + border),
                text=f"{cube}\n{wavelength:,.0f}nm",
                text_color="black",
                bbox_color="white",
                font=small_font,
                border=5,
            )

            frame = np.array(pil_frame)
            frames.append(frame)

        imageio.mimsave(dst, frames, duration=0.1)


def compute_cube_alignments(
    *,
    ref_path: Path,
    mov_path: Path,
    csv_path: Path,
    gif_path: Path,
    plot_path: Path,
    timestamp: int,
):
    ref = Stack.from_path(ref_path)
    mov = Stack.from_path(mov_path)

    stack_pair = StackPair(ref=ref, mov=mov)
    stack_pair.sharpen(sigma=1.0)
    stack_pair.compute_scales(upsample_factor=100)
    stack_pair.scale()
    stack_pair.compute_shifts()
    stack_pair.save_as_csv(csv_path)

    stack_pair.apply()
    stack_pair.save_as_gif(gif_path)

    _plot_cube_alignments(csv_path, plot_path, timestamp)


def run():
    """
    take as input im0.tiff, im1.tiff
    make csv with shifts and scales
    needs to run at the beginning of the nextflow pipeline and all other tasks must wait
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--ref-path", type=str, required=True)
    parser.add_argument("--mov-path", type=str, required=True)
    parser.add_argument("--csv-path", type=str, required=True)
    parser.add_argument("--gif-path", type=str, required=True)
    parser.add_argument("--plot-path", type=str, required=True)
    parser.add_argument("--timestamp", type=int, required=True)

    parser.add_argument("--n-cpus", type=int, default=None)
    parser.add_argument("--memory-limit", type=str, default=None)
    parser.add_argument("--dask-report-filename", type=str, default=None)

    args = parser.parse_args()

    with dask_session(
        memory_limit=args.memory_limit,
        n_cpus=args.n_cpus,
        dask_report_filename=args.dask_report_filename,
    ):
        compute_cube_alignments(
            ref_path=Path(args.ref_path),
            mov_path=Path(args.mov_path),
            csv_path=Path(args.csv_path),
            gif_path=Path(args.gif_path),
            plot_path=Path(args.plot_path),
            timestamp=args.timestamp,
        )
