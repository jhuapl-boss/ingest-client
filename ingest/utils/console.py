import sys


class WaitPrinter(object):
    """Simple class to handle a print while waiting
    """
    def __init__(self):
        self.first_print = True
        self.wait_char = "."

    def print_msg(self, msg):
        """Method to print an initial message"""
        if self.first_print:
            sys.stdout.write("{}{}{}{}".format(msg, self.wait_char, self.wait_char, self.wait_char))
            sys.stdout.flush()
            self.first_print = False
        else:
            sys.stdout.write(self.wait_char)
            sys.stdout.flush()

    def finished(self, msg=None):
        """Method to print final message"""
        if msg:
            print(msg)
        else:
            print("Complete")
