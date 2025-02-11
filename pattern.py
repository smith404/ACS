from enum import Enum
import json
import uuid

class Pattern:
    def __init__(self, duration=0):
        self.identifier = uuid.uuid4()
        self.slices = []
        self.duration = duration

    def add_slice(self, pattern_slice):
        if isinstance(pattern_slice, PatternSlice):
            pattern_slice.set_duration(self.duration)
            self.slices.append(pattern_slice)
        else:
            raise TypeError("Expected a PatternSlice instance")

    def set_identifier(self, identifier):
        self.identifier = identifier

    def set_duration(self, duration):
        self.duration = duration
        for slice in self.slices:
            slice.set_duration(duration)

    def align_slice_periods(self, development_periods=None):
        if development_periods is None or development_periods == 0:
            development_periods = len(self.slices)
        for index, slice in enumerate(self.slices, start=0):
            slice.set_development_periods(development_periods)
            slice.set_duration_offset(self.duration / development_periods)
            slice.set_start_offset(index * (self.duration / development_periods))

    def delete_slice(self, pattern_slice=None):
        if pattern_slice in self.slices:
            self.slices.remove(pattern_slice)
        else:
            raise ValueError("PatternSlice not found in slices")

    def expand(self):
        for index, slice in enumerate(self.slices, start=1):
            # Check for a start distribution
            if slice.start_distribution != 0:
                slice.iterate_start_periods()
            # Check for a distribution
            if slice.distribution != 0:
                slice.iterate_development_periods()

    def distribute_remaining(self):
        total_distribution = sum(slice.distribution + slice.start_distribution for slice in self.slices)
        if total_distribution < 1:
            remaining = 1 - total_distribution
            for slice in self.slices:
                slice.distribution += remaining / len(self.slices)

    def set_all_distributions_to_zero(self):
        for slice in self.slices:
            slice.distribution = 0

    def set_all_start_distributions_to_zero(self):
        for slice in self.slices:
            slice.start_distribution = 0

    def set_pattern_to_zero(self):
        self.set_all_start_distributions_to_zero()
        self.set_all_distributions_to_zero()

    def check_distribution(self):
        total_distribution = sum(slice.distribution + slice.start_distribution for slice in self.slices)
        return total_distribution == 1

    def check_durations(self):
        return all(slice.duration == self.duration for slice in self.slices)

    def get_all_pattern_blocks(self):
        blocks = []
        level = 0
        for slice in self.slices:
            blocks.extend(slice.get_pattern_blocks(self.identifier, level))
            if slice.start_distribution != 0:
                level = level + 1
            if slice.distribution != 0:
                level = level + 1
        return blocks

    def display(self):
        print(self)
        for slice in self.slices:
            print(slice)

    def __str__(self):
        return f"Pattern with {len(self.slices)} slices and duration {self.duration}"

class PatternSlice:
    def __init__(self, distribution=0, start_distribution=0, duration=0, start_offset=0, duration_offset=0, development_periods=0):
        self.distribution = distribution
        self.start_distribution = start_distribution
        self.duration = duration
        self.start_offset = start_offset
        self.duration_offset = duration_offset
        self.development_periods = development_periods

    def set_duration(self, duration):
        self.duration = duration

    def set_start_offset(self, start_offset):
        self.start_offset = start_offset

    def set_duration_offset(self, duration_offset):
        self.duration_offset = duration_offset

    def set_development_periods(self, development_periods):
        self.development_periods = development_periods

    def iterate_development_periods(self):
        for index in range(0, self.development_periods + 1):
            factor = self.development_periods
            # Check for the first and last index as these are half the value of the other indexes
            if index == 0 or index == self.development_periods:
                factor = factor * 2
            print(f"({self.start_offset+(index * self.duration_offset)} , {(self.start_offset+((index + 1) * self.duration_offset))-1} , {self.distribution/factor})", end='\t')
        # For a new line after the loop
        print()

    def iterate_start_periods(self):
        factor = self.development_periods
        for index in range(self.development_periods):
            print(f"({self.start_offset+(index * self.duration_offset)} , {(self.start_offset+((index + 1) * self.duration_offset))-1} , {self.start_distribution/factor})", end='\t')
        # For a new line after the loop
        print()

    def get_pattern_blocks(self, pattern_id, level=0):
        blocks = []
        if self.start_distribution != 0:
            for index in range(self.development_periods):
                shape = BlockShape.RECTANGLE
                start_point = self.start_offset + (index * self.duration_offset)
                end_point = self.start_offset + ((index + 1) * self.duration_offset) - 1
                block = PatternBlock(pattern_id, level=level, start_point=start_point, end_point=end_point, area=self.start_distribution / self.development_periods, shape=shape)
                blocks.append(block)
            level = level + 1
        if self.distribution != 0:
            for index in range(0, self.development_periods + 1):
                shape = BlockShape.RECTANGLE
                factor = self.development_periods
                # Check for the first and last index as these are half the value of the other indexes
                if index == 0 or index == self.development_periods:
                    factor = factor * 2
                    if index == 0:
                        shape = BlockShape.LTRIANGLE
                    else:
                        shape = BlockShape.RTRIANGLE
                start_point = self.start_offset+(index * self.duration_offset)
                end_point = (self.start_offset+((index + 1) * self.duration_offset))-1
                block = PatternBlock(pattern_id, level=level, start_point=start_point, end_point=end_point, area=self.distribution / factor, shape=shape)
                blocks.append(block)
        return sorted(blocks, key=lambda block: block.start_point)

    def __str__(self):
        return f"PatternSlice: (Distribution: {self.distribution}, Start Distribution: {self.start_distribution}, Duration: {self.duration}, Start Offset: {self.start_offset}, Duration Offset: {self.duration_offset}, Development Periods: {self.development_periods})"

class BlockShape(str, Enum):
    LTRIANGLE = "LTRIANGLE"
    RTRIANGLE = "RTRIANGLE"
    RECTANGLE = "RECTANGLE"

class PatternBlock:
    def __init__(self, pattern, level=0, start_point=0, end_point=0, area=0, shape=BlockShape.RECTANGLE):
        self.pattern = pattern
        self.level = level
        self.start_point = start_point
        self.end_point = end_point
        self.area = area
        self.shape = shape

    def generate_polygon(self, colour="blue", stroke="black", y_axis=0, height=50):
        if self.shape == BlockShape.RECTANGLE:
            points = f"{self.start_point},{y_axis} {self.end_point},{y_axis} {self.end_point},{y_axis + height} {self.start_point},{y_axis + height}"
        elif self.shape == BlockShape.LTRIANGLE:
            points = f"{self.start_point},{y_axis} {self.end_point},{y_axis} {self.end_point},{y_axis + height}"
        elif self.shape == BlockShape.RTRIANGLE:
            points = f"{self.start_point},{y_axis} {self.start_point},{y_axis + height} {self.end_point},{y_axis + height} "
        return f'<polygon points="{points}" fill="{colour}" stroke="{stroke}" />'

    def __str__(self):
        return f"PatternBlock with Pattern: {self.pattern}, Level: {self.level}, Start Point: {self.start_point}, End Point: {self.end_point}, Area: {self.area}, Shape: {self.shape.name}"

class PatternEvaluator:
    def __init__(self, pattern_blocks):
        if not all(isinstance(block, PatternBlock) for block in pattern_blocks):
            raise TypeError("All elements must be instances of PatternBlock")
        self.pattern_blocks = pattern_blocks

    def get_pattern_blocks_less_than(self, point):
        return [block for block in self.pattern_blocks if block.start_point < point]

    def get_pattern_blocks_greater_than(self, point):
        return [block for block in self.pattern_blocks if block.start_point > point]

    def get_pattern_blocks_between(self, start_point, end_point):
        return [block for block in self.pattern_blocks if start_point <= block.start_point < end_point]

    def create_svg(self, height = 50, x_cut = 0, y_cut = 0, pre_colour = "white", colour = "blue"):
        svg_elements = []
        for block in self.pattern_blocks:
            block_colour = colour
            if block.start_point < x_cut:
                block_colour = pre_colour
            if block.level < y_cut:
                block_colour = pre_colour
            element = block.generate_polygon(block_colour, y_axis=height*block.level, height=height)
            svg_elements.append(element)
        return f'<svg xmlns="http://www.w3.org/2000/svg">{"".join(svg_elements)}</svg>'

    def save_to_file(self, filename):
        with open(filename, 'w') as file:
            json.dump([block.__dict__ for block in self.pattern_blocks], file)

    @classmethod
    def load_from_file(cls, filename):
        with open(filename, 'r') as file:
            blocks_data = json.load(file)
            pattern_blocks = [PatternBlock(**block_data) for block_data in blocks_data]
            return cls(pattern_blocks)

    def evaluate(self):
        # Placeholder for evaluation logic
        pass

    def __str__(self):
        return f"PatternEvaluator with {len(self.pattern_blocks)} blocks"

def main():
    pattern = Pattern(360)
    pattern.set_identifier("Test Pattern")
    pattern.add_slice(PatternSlice(0, 0.1))
    pattern.add_slice(PatternSlice())
    pattern.add_slice(PatternSlice())
    pattern.add_slice(PatternSlice())

    pattern.distribute_remaining()
    pattern.align_slice_periods()

    pattern.display()
    print("Distribution check:", pattern.check_distribution())
    print("Duration check:", pattern.check_durations())

    evaluator = PatternEvaluator(pattern.get_all_pattern_blocks())
    for block in pattern.get_all_pattern_blocks():
        print(block)

    #evaluator.save_to_file("scratch/pattern.json")
    #loaded_evaluator = PatternEvaluator.load_from_file("scratch/pattern.json")
    print(evaluator.create_svg(x_cut=180, y_cut=2))

if __name__ == "__main__":
    main()