class Pattern:
    def __init__(self, duration=0):
        self.slices = []
        self.duration = duration

    def add_slice(self, pattern_slice):
        if isinstance(pattern_slice, PatternSlice):
            self.slices.append(pattern_slice)
        else:
            raise TypeError("Expected a PatternSlice instance")

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

    def display(self):
        print(self)
        for slice in self.slices:
            slice.display()

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

    def __str__(self):
        return f"Pattern with {len(self.slices)} slices and duration {self.duration}"

class PatternSlice:
    def __init__(self, duration, distribution=0, start_distribution=0, development_periods=0, start_offset=0, duration_offset=0):
        self.distribution = distribution
        self.duration = duration
        self.start_distribution = start_distribution
        self.development_periods = development_periods
        self.start_offset = start_offset
        self.duration_offset = duration_offset

    def set_duration(self, duration):
        self.duration = duration

    def set_start_offset(self, start_offset):
        self.start_offset = start_offset

    def set_duration_offset(self, duration_offset):
        self.duration_offset = duration_offset

    def set_development_periods(self, development_periods):
        self.development_periods = development_periods

    def display(self):
        print(self)

    def iterate_development_periods(self):
        for index in range(0, self.development_periods + 1):
            factor = self.development_periods
            if index == 0 or index == self.development_periods:
                factor = factor * 2
            print(f"{self.distribution/factor}", end='\t')
        # For a new line after the loop
        print()

    def iterate_start_periods(self):
        for _ in range(self.development_periods):
            print(self.start_distribution, end='\t')
        # For a new line after the loop
        print()

    def __str__(self):
        return f"PatternSlice: (Duration: {self.duration}, Distribution: {self.distribution}, Start Distribution: {self.start_distribution}, Development Periods: {self.development_periods}, Start Offset: {self.start_offset}, Duration Offset: {self.duration_offset})"

def main():
    pattern = Pattern(360)
    pattern.add_slice(PatternSlice(360, 0, 0.1284))
    pattern.add_slice(PatternSlice(360))
    pattern.add_slice(PatternSlice(360))
    pattern.add_slice(PatternSlice(360))

    pattern.display()        
    print("Distribution check:", pattern.check_distribution())
    print("Duration check:", pattern.check_durations())

    pattern.distribute_remaining()
    pattern.align_slice_periods()
    pattern.display()
    print("Distribution check:", pattern.check_distribution())
    print("Duration check:", pattern.check_durations())
    pattern.expand()

if __name__ == "__main__":
    main()