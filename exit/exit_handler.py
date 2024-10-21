import atexit

class ExitHandler:
    def __init__(self):
        self.functions = []

    def register(self, func, *args, **kwargs):
        self.functions.append((func, args, kwargs))

    def run(self):
        # Run the functions in the desired order
        for func, args, kwargs in self.functions:
            func(*args, **kwargs)

# Create a global exit handler instance
exit_handler = ExitHandler()

# Register the custom exit handler to run at exit
atexit.register(exit_handler.run)

# Function to register your desired function first
def register_first(func, *args, **kwargs):
    # Insert the function at the beginning of the list
    exit_handler.functions.insert(0, (func, args, kwargs))

# Function to register other functions
def register(func, *args, **kwargs):
    exit_handler.register(func, *args, **kwargs)