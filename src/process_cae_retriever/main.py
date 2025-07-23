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
import pprint
import traceback
import json

from .cli.module_log import Logger
from .utils.status_exception import StatusException
from .utils.module_prologo import prologo, epilogo

from .cae import _CAERetriever


@click.command()

# -----------------------------------------------------------------------------
# Specific options of your CLI application
# -----------------------------------------------------------------------------
@click.option(
    '--lat_range', 
    callback=lambda ctx, param, value: tuple(value) if value else None,
    type=float, nargs=2, default=None, 
    help="Latitude range as two floats (min, max)."
)
@click.option(
    '--long_range',
    callback=lambda ctx, param, value: tuple(value) if value else None,
    type=float, nargs=2, default=None,
    help="Longitude range as two floats (min, max)."
)
@click.option(
    '--time_range',
    callback=lambda ctx, param, value: tuple(value) if value else None,
    type=str, nargs=2, default=None,
    help="Time range as two ISO 8601 strings (start, end)."
)
@click.option(
    '--filters',
    type=str, default=None, 
    help="Filters to apply to the data."
)
@click.option(
    '--out',
    type=str, default=None,
    help="Output file path for the retrieved data. If not provided, the output will be returned as a dictionary."
)
@click.option(
    '--out_format',
    type=click.Choice(['geojson'], case_sensitive=False), default=None, 
    help="Output format of the retrieved data."
)
@click.option(
    '--bucket-destination',
    type=str, default=None, 
    help="Destination bucket for the output data."
)

# -----------------------------------------------------------------------------
# Common options to all Gecosistema CLI applications
# -----------------------------------------------------------------------------
@click.option(
    '--backend', 
    type=click.STRING, required=False, default=None,
    help="The backend to use for sending back progress status updates to the backend server."
)
@click.option(
    '--jid',
    type=click.STRING, required=False, default=None,
    help="The job ID to use for sending back progress status updates to the backend server. If not provided, it will be generated automatically."
)
@click.option(
    '--version',
    is_flag=True, required=False, default=False,
    help="Show the version of the package."
)
@click.option(
    '--debug',
    is_flag=True, required=False, default=False,
    help="Debug mode."
)
@click.option(
    '--verbose',
    is_flag=True, required=False, default=False,
    help="Print some words more about what is doing."
)

def cli_run_cae_retriever(**kwargs):
    """
    main_click - main function for the CLI application
    """
    output = run_cae_retriever(**kwargs)
    
    Logger.debug(pprint.pformat(output))
    
    return output


def run_cae_retriever(
    # --- Specific options ---
    lat_range = None,
    long_range = None,
    time_range = None,
    filters = None,
    out = None,
    out_format = None,
    bucket_destination = None,

    # --- Common options ---
    backend = None,
    jid = None,
    version = False,
    debug = False,
    verbose = False
):
    """
    main_python - main function
    """

    try:

        # DOC: -- Init logger + cli settings + handle version and debug -------
        t0, jid = prologo(backend, jid, version, verbose, debug)

        # DOC: -- Run the CAE retriever process -------------------------------
        CAERetriever = _CAERetriever()
        results = CAERetriever.run(
            lat_range=lat_range,
            long_range=long_range,
            time_range=time_range,
            filters=filters,
            out=out,
            out_format=out_format,
            bucket_destination=bucket_destination,
        )

    except StatusException as err:
        results = {
            'status': err.status,
            'body': {
                'message': str(err),
                ** ({"traceback": traceback.format_exc()} if debug else dict())
            }
        }
    except Exception as e:
        results = {
            "status": StatusException.ERROR,
            "body": {
                "error": str(e),
                ** ({"traceback": traceback.format_exc()} if debug else dict())
            }
        }

    # DOC: -- Cleanup the temporary files if needed ---------------------------
    epilogo(t0, backend, jid)
    
    return results
