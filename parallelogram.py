class Parallelogram:
    def __init__(self, area, length, bottom_offset):
        """
        Initialize a parallelogram with known area, length, and bottom offset.
        
        Args:
            area (float): The area of the parallelogram
            length (float): The length of both top and bottom sides
            bottom_offset (float): The horizontal distance the bottom is offset from the top
        """
        if area <= 0 or length <= 0:
            raise ValueError("Area and length must be positive values")
        
        self.area = area
        self.length = length
        self.bottom_offset = bottom_offset
        self.height = area / length  # Calculate height from area and length
    
    def get_corners(self):
        """
        Get the coordinates of all four corners of the parallelogram.
        Top left corner is always at (0, 0).
        
        Returns:
            dict: Dictionary with corner names as keys and (x, y) tuples as values
        """
        return {
            'top_left': (0, 0),
            'top_right': (self.length, 0),
            'bottom_left': (self.bottom_offset, -self.height),
            'bottom_right': (self.length + self.bottom_offset, -self.height)
        }
    
    def get_vertices(self):
        """
        Get the vertices as a list of (x, y) tuples in clockwise order.
        
        Returns:
            list: List of (x, y) coordinate tuples
        """
        corners = self.get_corners()
        return [
            corners['top_left'],
            corners['top_right'],
            corners['bottom_right'],
            corners['bottom_left']
        ]
    
    def get_width(self):
        """
        Get the total width of the parallelogram (maximum x-coordinate).
        
        Returns:
            float: The width of the parallelogram
        """
        corners = self.get_corners()
        x_coords = [corner[0] for corner in corners.values()]
        return max(x_coords) - min(x_coords)
    
    def __str__(self):
        return f"Parallelogram(area={self.area}, length={self.length}, height={self.height}, bottom_offset={self.bottom_offset})"
    
    def __repr__(self):
        return self.__str__()
    
    def get_area_between_lines(self, line1, line2, orientation='vertical'):
        """
        Calculate the area of the parallelogram section between two cutting lines.
        
        Args:
            line1 (float): Position of first cutting line
            line2 (float): Position of second cutting line
            orientation (str): 'vertical' for x-coordinates or 'horizontal' for y-coordinates
        
        Returns:
            float: Area of the section between the two lines
        """
        if orientation not in ['vertical', 'horizontal']:
            raise ValueError("Orientation must be 'vertical' or 'horizontal'")
        
        # Ensure line1 <= line2 for consistent calculation
        if line1 > line2:
            line1, line2 = line2, line1
        
        if orientation == 'vertical':
            return self._get_area_between_vertical_lines(line1, line2)
        else:
            return self._get_area_between_horizontal_lines(line1, line2)
    
    def _get_area_between_vertical_lines(self, x1, x2):
        """Calculate area between two vertical lines at x1 and x2."""
        corners = self.get_corners()
        
        # Get parallelogram boundaries
        left_bound = min(corners['top_left'][0], corners['bottom_left'][0])
        right_bound = max(corners['top_right'][0], corners['bottom_right'][0])
        
        # Clamp lines to parallelogram boundaries
        x1 = max(x1, left_bound)
        x2 = min(x2, right_bound)
        
        if x1 >= x2:
            return 0.0
        
        # For a parallelogram, the area between vertical lines is proportional
        # to the width ratio times the total area
        total_width = right_bound - left_bound
        section_width = x2 - x1
        
        return (section_width / total_width) * self.area
    
    def _get_area_between_horizontal_lines(self, y1, y2):
        """Calculate area between two horizontal lines at y1 and y2."""
        corners = self.get_corners()
        
        # Get parallelogram boundaries (y increases upward, but our parallelogram goes down)
        top_bound = max(corners['top_left'][1], corners['top_right'][1])  # Should be 0
        bottom_bound = min(corners['bottom_left'][1], corners['bottom_right'][1])  # Should be -height
        
        # Clamp lines to parallelogram boundaries
        y1 = max(y1, bottom_bound)
        y2 = min(y2, top_bound)
        
        if y1 >= y2:
            return 0.0
        
        # For a parallelogram, the area between horizontal lines is proportional
        # to the height ratio times the total area
        total_height = top_bound - bottom_bound
        section_height = y2 - y1
        
        return (section_height / total_height) * self.area

import unittest

class TestParallelogram(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.para1 = Parallelogram(area=20, length=5, bottom_offset=2)
        self.para2 = Parallelogram(area=12, length=4, bottom_offset=0)  # Rectangle
        self.para3 = Parallelogram(area=15, length=3, bottom_offset=-1)  # Negative offset
    
    def test_initialization(self):
        """Test proper initialization of parallelogram."""
        self.assertEqual(self.para1.area, 20)
        self.assertEqual(self.para1.length, 5)
        self.assertEqual(self.para1.bottom_offset, 2)
        self.assertEqual(self.para1.height, 4)  # 20/5 = 4
    
    def test_initialization_validation(self):
        """Test validation of input parameters."""
        with self.assertRaises(ValueError):
            Parallelogram(area=-10, length=5, bottom_offset=0)
        with self.assertRaises(ValueError):
            Parallelogram(area=10, length=0, bottom_offset=0)
    
    def test_get_corners(self):
        """Test corner coordinate calculation."""
        corners = self.para1.get_corners()
        expected = {
            'top_left': (0, 0),
            'top_right': (5, 0),
            'bottom_left': (2, -4),
            'bottom_right': (7, -4)
        }
        self.assertEqual(corners, expected)
    
    def test_get_corners_rectangle(self):
        """Test corner coordinates for a rectangle (offset = 0)."""
        corners = self.para2.get_corners()
        expected = {
            'top_left': (0, 0),
            'top_right': (4, 0),
            'bottom_left': (0, -3),  # 12/4 = 3 height
            'bottom_right': (4, -3)
        }
        self.assertEqual(corners, expected)
    
    def test_get_vertices(self):
        """Test vertex order (clockwise)."""
        vertices = self.para1.get_vertices()
        expected = [(0, 0), (5, 0), (7, -4), (2, -4)]
        self.assertEqual(vertices, expected)
    
    def test_get_width(self):
        """Test width calculation."""
        self.assertEqual(self.para1.get_width(), 7)  # max(0,5,7,2) - min(0,5,7,2) = 7-0
        self.assertEqual(self.para2.get_width(), 4)  # Rectangle width
        self.assertEqual(self.para3.get_width(), 3)  # Negative offset case
    
    def test_area_between_vertical_lines(self):
        """Test area calculation between vertical lines."""
        # Full width should equal total area
        area = self.para1.get_area_between_lines(0, 7, 'vertical')
        self.assertAlmostEqual(area, 20, places=5)
        
        # Half width should be half area
        area = self.para1.get_area_between_lines(0, 3.5, 'vertical')
        self.assertAlmostEqual(area, 10, places=5)
        
        # Outside bounds should return 0
        area = self.para1.get_area_between_lines(-5, -1, 'vertical')
        self.assertEqual(area, 0)
    
    def test_area_between_horizontal_lines(self):
        """Test area calculation between horizontal lines."""
        # Full height should equal total area
        area = self.para1.get_area_between_lines(-4, 0, 'horizontal')
        self.assertAlmostEqual(area, 20, places=5)
        
        # Half height should be half area
        area = self.para1.get_area_between_lines(-2, 0, 'horizontal')
        self.assertAlmostEqual(area, 10, places=5)
        
        # Outside bounds should return 0
        area = self.para1.get_area_between_lines(1, 5, 'horizontal')
        self.assertEqual(area, 0)
    
    def test_area_between_lines_swapped_order(self):
        """Test that line order doesn't matter."""
        area1 = self.para1.get_area_between_lines(1, 3, 'vertical')
        area2 = self.para1.get_area_between_lines(3, 1, 'vertical')
        self.assertAlmostEqual(area1, area2, places=5)
    
    def test_invalid_orientation(self):
        """Test invalid orientation parameter."""
        with self.assertRaises(ValueError):
            self.para1.get_area_between_lines(0, 1, 'diagonal')
    
    def test_string_representation(self):
        """Test string representation methods."""
        expected = "Parallelogram(area=20, length=5, height=4.0, bottom_offset=2)"
        self.assertEqual(str(self.para1), expected)
        self.assertEqual(repr(self.para1), expected)

if __name__ == '__main__':
    para = Parallelogram(area=12.84, length=365, bottom_offset=92)
    print(para.get_area_between_lines(0, 1, 'vertical'))
    print(para.get_area_between_lines(1, 2, 'vertical'))
    print(para.get_area_between_lines(0, 2, 'vertical'))
