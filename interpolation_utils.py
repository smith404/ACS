from scipy.interpolate import interp1d
import numpy as np

def spline_interpolation(points_values: dict[int, float], x: int) -> float:
    points = sorted(points_values.keys())
    values = [points_values[point] for point in points]
    
    spline = interp1d(points, values, kind='cubic', fill_value="extrapolate")
    return float(spline(x))
