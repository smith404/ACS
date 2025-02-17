# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.

from scipy.interpolate import interp1d
import numpy as np

def spline_interpolation(points_values: dict[int, float], x: int) -> float:
    points = sorted(points_values.keys())
    values = [points_values[point] for point in points]
    
    spline = interp1d(points, values, kind='cubic', fill_value="extrapolate")
    return float(spline(x))

def dict_to_coordinates(data: dict[int, float]) -> list[tuple[int, float]]:
    return [(key, value) for key, value in data.items()]
