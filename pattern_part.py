import matplotlib.pyplot as plt

class PatternPart:
    def __init__(self, top_length, height):
        """
        Initialize a pattern part as a parallelogram.
        
        Args:
            top_length (float): The length of the top side
            height (float): The height of the parallelogram (also the horizontal offset)
        """
        if top_length <= 0 or height <= 0:
            raise ValueError("Top length and height must be positive values")
        
        self.top_length = top_length
        self.height = height
        self.area = top_length * height  # Area of parallelogram
    
    def get_corners(self):
        """
        Get the coordinates of all four corners of the parallelogram.
        Top left corner is at (0, 0).
        
        Returns:
            dict: Dictionary with corner names as keys and (x, y) tuples as values
        """
        return {
            'top_left': (0, 0),
            'top_right': (self.top_length, 0),
            'bottom_left': (self.height, self.height),
            'bottom_right': (self.top_length + self.height, self.height)
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
        Get the total width of the parallelogram.
        
        Returns:
            float: The width of the parallelogram
        """
        return self.top_length + self.height
    
    def get_height(self):
        """
        Get the total width of the parallelogram.
        
        Returns:
            float: The width of the parallelogram
        """
        return self.height
    
    def get_area(self):
        """
        Get the area of the parallelogram.
        
        Returns:
            float: The area of the parallelogram
        """
        return self.area
    
    def get_cut_area(self, h_cut, v_cut, region):
        """
        Calculate the area of the parallelogram based on cutting points and region.

        Args:
            h_cut (float): Horizontal cutting point (y coordinate)
            v_cut (float): Vertical cutting point (x coordinate)
            region (str): One of 'above', 'below', 'left', 'right',
                          'top_left', 'top_right', 'bottom_left', 'bottom_right'

        Returns:
            float: Area of the specified region

        Note:
            - Assumes parallelogram is axis-aligned as defined by get_corners().
            - Cutting points outside the parallelogram will return 0 or full area.
        """
        x0, y0 = 0, 0
        x1 = self.top_length + self.height
        y1 = self.height

        def clamp(val, minv, maxv):
            return max(minv, min(val, maxv))

        # Area above horizontal cut
        if region == 'above':
            if h_cut <= y0:
                return self.area
            if h_cut >= y1:
                return 0
            return self.top_length * (y1 - h_cut)

        # Area below horizontal cut
        elif region == 'below':
            if h_cut <= y0:
                return 0
            if h_cut >= y1:
                return self.area
            return self.top_length * (h_cut - y0)

        # Area left of vertical cut
        elif region == 'left':
            if v_cut <= x0:
                return 0
            if v_cut >= x1:
                return self.area
            # Only count the parallelogram region to the left of v_cut
            total = 0.0
            steps = 1000
            dy = self.height / steps
            for i in range(steps):
                y = i * dy
                left_x = y
                right_x = self.top_length + y
                x_start = left_x
                x_end = min(v_cut, right_x)
                width = max(0, x_end - x_start)
                total += width * dy
            return total

        # Area right of vertical cut
        elif region == 'right':
            if v_cut <= x0:
                return self.area
            if v_cut >= x1:
                return 0
            total = 0.0
            steps = 1000
            dy = self.height / steps
            for i in range(steps):
                y = i * dy
                left_x = y
                right_x = self.top_length + y
                x_start = max(v_cut, left_x)
                x_end = right_x
                width = max(0, x_end - x_start)
                total += width * dy
            return total

        # Area at top left of cutting points
        elif region == 'top_left':
            if h_cut >= self.height or v_cut <= 0:
                return 0
            y_start = max(h_cut, 0)
            y_end = self.height
            steps = 1000
            dy = (y_end - y_start) / steps if y_end > y_start else 0
            total = 0.0
            if dy > 0:
                for i in range(steps):
                    y = y_start + i * dy
                    left_x = y
                    right_x = self.top_length + y
                    # Only integrate where left_x < min(v_cut, right_x)
                    x_end = min(v_cut, right_x)
                    if x_end > left_x:
                        width = x_end - left_x
                        total += width * dy
            return total

        # Area at top right of cutting points
        elif region == 'top_right':
            if h_cut >= self.height or v_cut >= self.top_length + self.height:
                return 0
            y_start = max(h_cut, 0)
            y_end = self.height
            steps = 1000
            dy = (y_end - y_start) / steps if y_end > y_start else 0
            total = 0.0
            if dy > 0:
                for i in range(steps):
                    y = y_start + i * dy
                    left_x = y
                    right_x = self.top_length + y
                    x_start = max(v_cut, left_x)
                    x_end = right_x
                    width = max(0, x_end - x_start)
                    total += width * dy
            return total

        # Area at bottom left of cutting points
        elif region == 'bottom_left':
            if h_cut <= 0 or v_cut <= 0:
                return 0
            y_start = 0
            y_end = min(h_cut, self.height)
            steps = 1000
            dy = (y_end - y_start) / steps if y_end > y_start else 0
            total = 0.0
            if dy > 0:
                for i in range(steps):
                    y = y_start + i * dy
                    left_x = y
                    right_x = self.top_length + y
                    x_start = left_x
                    x_end = min(v_cut, right_x)
                    width = max(0, x_end - x_start)
                    total += width * dy
            return total

        # Area at bottom right of cutting points
        elif region == 'bottom_right':
            if h_cut <= 0 or v_cut >= self.top_length + self.height:
                return 0
            y_start = 0
            y_end = min(h_cut, self.height)
            steps = 1000
            dy = (y_end - y_start) / steps if y_end > y_start else 0
            total = 0.0
            if dy > 0:
                for i in range(steps):
                    y = y_start + i * dy
                    left_x = y
                    right_x = self.top_length + y
                    x_start = max(v_cut, left_x)
                    x_end = right_x
                    width = max(0, x_end - x_start)
                    total += width * dy
            return total

        else:
            raise ValueError("Invalid region specified")
    
    def __str__(self):
        return f"PatternPart(top_length={self.top_length}, height={self.height}, area={self.area})"
    
    def __repr__(self):
        return self.__str__()

    def visualize(self, ax=None, color='skyblue', edgecolor='black', alpha=0.5, show=True):
        """
        Visualize the parallelogram using matplotlib.

        Args:
            ax: matplotlib axes object (optional)
            color: fill color
            edgecolor: edge color
            alpha: transparency
            show: whether to call plt.show()
        """
        vertices = self.get_vertices()
        xs, ys = zip(*vertices)
        xs = list(xs) + [xs[0]]
        ys = list(ys) + [ys[0]]

        if ax is None:
            fig, ax = plt.subplots()
        ax.fill(xs, ys, color=color, edgecolor=edgecolor, alpha=alpha)
        ax.plot(xs, ys, color=edgecolor)
        ax.set_aspect('equal')
        ax.set_title('Parallelogram')
        ax.set_xlabel('X')
        ax.set_ylabel('Y')
        ax.grid(True)
        if show:
            plt.show()

if __name__ == '__main__':
    # Example usage
    part = PatternPart(top_length=365, height=92)
    print(part)
    print(f"Corners: {part.get_corners()}")
    print(f"Vertices: {part.get_vertices()}")
    print(f"Width: {part.get_width()}")
    print(f"Height: {part.get_height()}")
    print(f"Area: {part.get_area()}")
    print(f"Cut Area (left, h_cut=92, v_cut=92): {part.get_cut_area(92, 92, 'left')}")
    print(f"Cut Area (right, h_cut=92, v_cut=92): {part.get_cut_area(92, 92, 'right')}")
    print(f"Cut Area (above, h_cut=92, v_cut=92): {part.get_cut_area(92, 92, 'above')}")
    print(f"Cut Area (below, h_cut=92, v_cut=92): {part.get_cut_area(92, 92, 'below')}")

    print(f"Cut Area (top left, h_cut=1, v_cut=1): {part.get_cut_area(1, 1, 'top_left')}")
    print(f"Cut Area (top left, h_cut=2, v_cut=2): {part.get_cut_area(2, 2, 'top_left')}")
    print(f"Cut Area (above, h_cut=92, v_cut=92): {part.get_cut_area(92, 92, 'above')}")
    print(f"Cut Area (below, h_cut=92, v_cut=92): {part.get_cut_area(92, 92, 'below')}")

    print(f"Cut Area (top left, h_cut=1, v_cut=1): {part.get_cut_area(1, 1, 'top_left')}")
    print(f"Cut Area (top left, h_cut=2, v_cut=2): {part.get_cut_area(2, 2, 'top_left')}")
    print(f"Cut Area (top left, h_cut=92, v_cut=92): {part.get_cut_area(92, 92, 'top_left')}")

    part.visualize()
