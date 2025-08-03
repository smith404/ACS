# Copyright (c) 2025 K2-Software GmbH
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the licence conditions.

def save_string_to_file(filename: str, content: str) -> None:
    with open(filename, 'w') as file:
        file.write(content)

def scale_vector_to_sum(data: dict[int, float], target_sum: int) -> dict[int, int]:
    current_sum = sum(data.values())
    if current_sum == 0:
        raise ValueError("The sum of the vector elements is zero, cannot scale.")
    scale_factor = target_sum / current_sum
    return {key: round(value * scale_factor) for key, value in data.items()}

def cumulative_sum(data: dict[int, int]) -> dict[int, int]:
    cumulative = 0
    result = {}
    for key in sorted(data.keys()):
        cumulative += data[key]
        result[key] = cumulative
    return result
