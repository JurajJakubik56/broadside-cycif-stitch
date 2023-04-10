import re

re_dark_timestamps = re.compile("dark-(?P<ts>[0-9]+).ome.tif{1,2}")
re_filter = re.compile("^Filter:(?P<cube>[A-Za-z0-9-+_]+)-Emission$")
re_wavelength = re.compile(
    "^Filter:(?P<wavelength>[0-9]+)nm-(?P<bandwidth>10|20)nm-Emission$"
)
