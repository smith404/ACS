# Simple Pattern Classes

This module provides classes to create and manage patterns and their slices. Below is a brief description of each class and its functionality.

## Classes

### Pattern

Represents a pattern consisting of multiple slices.

#### Attributes
- `identifier`: A unique identifier for the pattern, initialized to a UUID.
- `slices`: A list of `PatternSlice` instances.
- `duration`: The total duration of the pattern.

#### Methods
- `__init__(self, duration=0)`: Initializes a new pattern with the given duration.
- `add_slice(self, pattern_slice)`: Adds a `PatternSlice` to the pattern.
- `set_identifier(self, identifier)`: Sets the identifier of the pattern.
- `set_duration(self, duration)`: Sets the duration of the pattern and updates all slices.
- `align_slice_periods(self, development_periods=None)`: Aligns the periods of all slices.
- `delete_slice(self, pattern_slice=None)`: Deletes a slice from the pattern.
- `expand(self)`: Expands the pattern by iterating over the slices.
- `distribute_remaining(self)`: Distributes the remaining distribution among slices.
- `set_all_distributions_to_zero(self)`: Sets all slice distributions to zero.
- `set_all_start_distributions_to_zero(self)`: Sets all slice start distributions to zero.
- `set_pattern_to_zero(self)`: Sets all distributions and start distributions to zero.
- `check_distribution(self)`: Checks if the total distribution equals 1.
- `check_durations(self)`: Checks if all slices have the same duration as the pattern.
- `get_all_pattern_blocks(self)`: Returns all pattern blocks sorted by start point.
- `get_pattern_blocks_less_than(self, point)`: Returns pattern blocks with start points less than the given point.
- `get_pattern_blocks_greater_than(self, point)`: Returns pattern blocks with start points greater than the given point.
- `get_pattern_blocks_between(self, start_point, end_point)`: Returns pattern blocks between the given start and end points.
- `display(self)`: Displays the pattern and its slices.
- `__str__(self)`: Returns a string representation of the pattern.

### PatternSlice

Represents a slice of a pattern.

#### Attributes
- `distribution`: The distribution value of the slice.
- `start_distribution`: The start distribution value of the slice.
- `duration`: The duration of the slice.
- `start_offset`: The start offset of the slice.
- `duration_offset`: The duration offset of the slice.
- `development_periods`: The number of development periods for the slice.

#### Methods
- `__init__(self, distribution=0, start_distribution=0, duration=0, start_offset=0, duration_offset=0, development_periods=0)`: Initializes a new slice with the given parameters.
- `set_duration(self, duration)`: Sets the duration of the slice.
- `set_start_offset(self, start_offset)`: Sets the start offset of the slice.
- `set_duration_offset(self, duration_offset)`: Sets the duration offset of the slice.
- `set_development_periods(self, development_periods)`: Sets the development periods of the slice.
- `iterate_development_periods(self)`: Iterates over the development periods and prints the distribution.
- `iterate_start_periods(self)`: Iterates over the start periods and prints the start distribution.
- `get_pattern_blocks(self, pattern_id)`: Returns the pattern blocks for the slice.
- `__str__(self)`: Returns a string representation of the slice.

### BlockShape

An enumeration representing the shape of a pattern block.

#### Values
- `TRIANGLE`: Represents a triangular block shape.
- `RECTANGLE`: Represents a rectangular block shape.

### PatternBlock

Represents a block within a pattern slice.

#### Attributes
- `pattern_id`: The identifier of the pattern to which the block belongs.
- `start_point`: The start point of the block.
- `end_point`: The end point of the block.
- `area`: The area of the block.
- `shape`: The shape of the block.

#### Methods
- `__init__(self, pattern_id, start_point=0, end_point=0, area=0, shape=BlockShape.RECTANGLE)`: Initializes a new block with the given parameters.
- `__str__(self)`: Returns a string representation of the block.