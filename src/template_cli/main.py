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
# Name:        main.py
# Purpose:
#
# Author:      Luzzi Valerio
#
# Created:     18/03/2021
# -----------------------------------------------------------------------------
import click

from .module_s3 import copy
from .module_args import check_args
from .module_prologo import prologo, epilogo


@click.command()
# -----------------------------------------------------------------------------
# Specific options of your CLI application
# -----------------------------------------------------------------------------
@click.option('--dem',   type=click.STRING, required=False, help="The input pathname.")
@click.option('--water', type=click.STRING, required=False, help="The input pathname.")
@click.option('--bbox',  type=click.STRING, help="The input pathname.")
@click.option('--out',   type=click.STRING, required=False, default=None, help="The output file name.")
# -----------------------------------------------------------------------------
# Common options to all Gecosistema CLI applications
# -----------------------------------------------------------------------------
@click.option('--backend', type=click.STRING, required=False, default=None,
              help="The backend to use for sending back progress status updates "
              "to the backend server.")
@click.option('--jid', type=click.STRING, required=False, default=None,
              help="The job ID to use for sending back progress status updates "
              "to the backend server. If not provided, it will be generated automatically.")
@click.option('--version', is_flag=True, required=False, default=False,
              help="Show the version of the package.")
@click.option('--debug', is_flag=True, required=False, default=False,
              help="Debug mode.")
@click.option('--verbose', is_flag=True, required=False, default=False,
              help="Print some words more about what is doing.")
def main_click(**kwargs):
    """
    Bla bla bla ...
    """
    return main_python(**kwargs)


def main_python(dem,
                water=None,
                out=None,
                bbox=None,
                # --- Common options ---
                backend=None,
                jid=None,
                version=False,
                debug=False,
                verbose=False):
    """
    main_python - main function
    """
    # -- Init the logger ---
    # set jid to the process ID if not provided
    # manage --version --verbose --debug options
    t0, jid = prologo(backend, jid, version, verbose, debug)

    # -- The arguments check id deferred here to be shared with lambda functions 
    # or other entry points that may not use Click
    res = check_args(dem, water, out, bbox)
    if res and res["statusCode"] != 200:
        epilogo(t0, backend, jid)
        return res

    # -- Do the job!! ---
    click.echo(click.style("Hello world!", fg="bright_green", bold=True))

    filedem = copy(dem)
    click.echo(click.style(
        f"Copied file: {filedem}", fg="bright_blue", bold=True))

    filewd = copy(water)
    click.echo(click.style(
        f"Copied file: {filewd}", fg="bright_blue", bold=True))
    # ---

    # -------------------------------------------------------------------------
    # Cleanup the temporary files if needed
    epilogo(t0, backend, jid)

    return res
