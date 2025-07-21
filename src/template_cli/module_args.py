# -----------------------------------------------------------------------------
# License:
# Copyright (c) 2025 Gecosistema S.r.l.
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
#
# Name:        module_args.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     25/06/2025
# -----------------------------------------------------------------------------
from .module_log import Logger
from .module_s3 import isfile


def check_args(dem, water, out, bbox):
    """
    Check if the provided arguments are valid.
    :param args: List of arguments to check.
    :return: True if all arguments are valid, False otherwise.
    """
    res = {
        "statusCode": 200,
        "body": {
            "message": "Arguments are valid",
        }
    }


    Logger.info("Checking arguments...")

    if not isfile(dem):
        res["statusCode"] = 400
        res["body"]["message"] = f"DEM file not found: {dem}"
        Logger.error(res["body"]["message"])
        return res

    if not isfile(water):
        res["statusCode"] = 400
        res["body"]["message"] = f"Water mask file not found: {water}"
        Logger.error(res["body"]["message"])
        return res


    return res