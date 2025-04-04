def __init__(self):
    """Initialize the visualizer."""
    self.output_dir = OUTPUT_DIR
    os.makedirs(self.output_dir, exist_ok=True)