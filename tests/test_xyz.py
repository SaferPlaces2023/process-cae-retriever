import unittest
from template_cli import main_python


class Test(unittest.TestCase):
    """
    Test class for the greenamptr module.
    """

    def test_xyz(self):
        """
        test_xyz is a test method that checks if the main function runs without errors.
        """
        res = main_python(dem=None, out=None, debug=False, verbose=False)
        # Check if the result is True)
        self.assertEqual(res, 0, "The main function should return 0 when executed without errors.")


if __name__ == '__main__':
    unittest.main()
