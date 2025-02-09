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

    def expand(self):
        for index, slice in enumerate(self.slices, start=1):
            print(f"Slice {index}: {slice}")

    def check_distribution(self):
        total_distribution = sum(slice.distribution + slice.start_distribution for slice in self.slices)
        return total_distribution == 1

    def distribute_remaining(self):
        total_distribution = sum(slice.distribution + slice.start_distribution for slice in self.slices)
        if (total_distribution < 1):
            remaining = 1 - total_distribution
            for slice in self.slices:
                slice.distribution += remaining/len(self.slices)

class PatternSlice:
    def __init__(self, distribution, duration, start_distribution):
        self.distribution = distribution
        self.duration = duration
        self.start_distribution = start_distribution

    def set_duration(self, duration):
        self.duration = duration

    def display(self):
        print(f"PatternSlice: (Duration: {self.duration}, Distribution: {self.distribution}, Start Distribution: {self.start_distribution})")

    def __str__(self):
        return f"PatternSlice: (Duration: {self.duration}, Distribution: {self.distribution}, Start Distribution: {self.start_distribution})"

def main():
    pattern = Pattern()
    pattern.add_slice(PatternSlice(0.2, 90, 0.15))
    pattern.add_slice(PatternSlice(0.15, 90, 0))
    pattern.add_slice(PatternSlice(0.1, 90, 0))
    pattern.add_slice(PatternSlice(0.15, 90, 0))

    pattern.expand()
    print("Distribution check:", pattern.check_distribution())
    pattern.distribute_remaining()
    pattern.expand()
    print("Distribution check:", pattern.check_distribution())

if __name__ == "__main__":
    main()