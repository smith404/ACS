class Pattern:
    def __init__(self):
        self.slices = []

    def add_slice(self, pattern_slice):
        if isinstance(pattern_slice, PatternSlice):
            self.slices.append(pattern_slice)
        else:
            raise TypeError("Expected a PatternSlice instance")

    def set_duration(self, duration):
        for slice in self.slices:
            slice.set_duration(duration)

    def delete_slice(self, pattern_slice):
        if pattern_slice in self.slices:
            self.slices.remove(pattern_slice)
        else:
            raise ValueError("PatternSlice not found in slices")

    def display(self):
        print(self.pattern)
        for slice in self.slices:
            slice.display()

    def exapand(self):
        for slice in self.slices:
            print(slice)

class PatternSlice:
    def __init__(self, distribution, duration, start_distribution):
        self.distribution = distribution
        self.duration = duration
        self.start_distribution = start_distribution

    def set_duration(self, duration):
        self.duration = duration

    def display(self):
        print(f"PatternSlice: (Duration: {self.duration}, Distribution: {self.distribution}, Start Distribution: {self.start_distribution})")