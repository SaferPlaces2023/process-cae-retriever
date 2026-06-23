# =================================================================
#
# Authors: Valerio Luzzi <valluzzi@gmail.com>
#
# Copyright (c) 2023 Valerio Luzzi
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
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
# =================================================================

import os
import json
import uuid
import datetime
import requests

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError


from ..cli.module_log import Logger, set_log_debug
from ..utils import filesystem, module_s3
from ..utils.status_exception import StatusException

from .cae_retriever import _CAERetriever



PROCESS_METADATA = {
    'version': '0.2.0',
    'id': 'cae_retriever_process',
    'title': {
        'en': 'CAE Retriever Process',
    },
    'description': {
        'en': 'Process to retrieve data from the CAE seonsors.',
    },
    'jobControlOptions': ['sync-execute', 'async-execute'],
    'keywords': ['CAE', 'retriever', 'process', 'sensor', 'pygeoapi'],

    'inputs': {
        'token': {
            'title': 'secret token',
            'description': 'identify yourself',
            'schema': {
                'type': 'string'
            }
        },

        'lat_range': {
            'title': 'Latitude range',
            'description': 'The latitude range in format [lat_min, lat_max]. Values must be in EPSG:4326 crs. If no latitude range is provided, all latitudes will be returned',
            'schema': {
            }
        },
        'long_range': {
            'title': 'Longitude range',
            'description': 'The longitude range in format [long_min, long_max]. Values must be in EPSG:4326 crs. If no longitude range is provided, all longitudes will be returned',
            'schema': {
            }
        },
        'time_range': {
            'title': 'Time range',
            'description': 'The time range in format [time_start, time_end]. Both time_start and time_end must be in ISO-Format and related to at least one week ago. If no time range is provided, all times will be returned',
            'schema': {
            }
        },

        'filters': {
            'title': 'Filters',
            'description': 'Filters to apply to the data. If no filters are provided, all data will be returned. The filters can be a single value or a list of values. The filters are applied to the station, element, instrument and quantity fields.',
            'schema': {
                'type': 'object',
                'properties': {
                    'station': {
                        'title': 'Station',
                        'description': 'The station to filter the data. If no station is provided, all stations will be returned',
                        'type': 'string or list of strings',
                        'default': None
                    },
                    'element': {
                        'title': 'Element',
                        'description': 'The element (sensor) to filter the data. If no element is provided, all elements will be returned',
                        'type': 'string or list of strings',
                        'default': None
                    },
                    'instrument': {
                        'title': 'Instrument',
                        'description': 'The instrument to filter the data. If no instrument is provided, all instruments will be returned',
                        'type': 'string or list of strings',
                        'default': None
                    },
                    'quantity': {
                        'title': 'Quantity',
                        'description': 'The quantity to filter the data. If no quantity is provided, all quantities will be returned',
                        'type': 'string or list of strings',
                        'default': None
                    },
                },
            }
        },

        'out': {
            'title': 'Output file path',
            'description': 'The output file path for the retrieved data. If neither out nor bucket_destination are provided, the output will be returned as a feature collection.',
            'schema': {
                'type': 'string'
            }
        },
        'out_format': {
            'title': 'Return format type',
            'description': 'The return format type. Possible values are "geojson" or "dataframe". "geojson" is default and preferable.',
            'schema': {
            }
        }, 
        'bucket_destination': {
            'title': 'Bucket destination',
            'description': 'The bucket destination where the data will be stored. If not provided, the data will not be stored in a bucket. If neither out nor bucket_destination are provided, the output will be returned as a feature collection.',
            'schema': {
                'type': 'string'
            }
        },

        'debug': {
            'title': 'Debug',
            'description': 'Enable Debug mode. Can be valued as true or false',
            'schema': {
            }
        }
    },

    'outputs': {
        'id': {
            'title': 'ID',
            'description': 'The ID of the process execution',
            'schema': {
            }
        },
    },

    'example': {
        "inputs": {
            'token': 'your_secret_token',
            'lat_range': [ 43.92, 44.77 ],
            'long_range': [ 12.20, 12.83 ],
            'time_range': ['2025-07-23T10:00:00', '2025-07-23T12:00:00'],
            'filters': {
                'station': ['station1', 'station2'],
                'element': ['element1', 'element2'],
                'instrument': ['instrument1'],
                'quantity': ['quantity1', 'quantity2']
            },
            'out': 'path/to/output/file.geojson',
            'out_format': 'geojson',
            'bucket_destination': 's3://your-bucket-name/store/data/prefix',
            'debug': True
        }
    }
}


class CAERetrieverProcessor(BaseProcessor):
    """
    CAE Retriever Process Processor
    """

    def __init__(self, processor_def):
        """
        Initialize the CAE Retriever Processor.
        """

        super().__init__(processor_def, PROCESS_METADATA)

        self.name = 'CAERetrieverProcessor'

        # REF: https://arpaebo.caedns.it/platform/api-doc/index.html (Swagger API Documentation)
        self.base_urls = 'https://arpaebo.caedns.it/platform'
        self.auth_url = f'{self.base_urls}/connect/token'
        self.sensor_list_url = f'{self.base_urls}/v1/elements'
        self.location_url = f'{self.base_urls}/v1/locations'
        self.sensor_specs_url = lambda sensor_id: f'{self.base_urls}/v2/elements/{sensor_id}'
        self.sensor_data_url = lambda sensor_id: f'{self.base_urls}/v1/data/{sensor_id}'

        self._tmp_data_folder = os.path.join(os.getcwd(), f'{self.name}_tmp')
        if not os.path.exists(self._tmp_data_folder):
            os.makedirs(self._tmp_data_folder)

        self._cache_data_folder = os.path.join(os.getcwd(), f'{self.name}_cache')
        if not os.path.exists(self._cache_data_folder):
            os.makedirs(self._cache_data_folder)

        # Dual-mode configuration
        self.processor_mode = os.getenv('CAE_PROCESSOR_MODE', 'local').lower()
        if self.processor_mode not in ['local', 'lambda']:
            self.processor_mode = 'local'
        Logger.debug(f'CAE Processor mode: {self.processor_mode}')

        # Lambda configuration (only loaded if mode is lambda)
        self._lambda_client = None
        self._lambda_function_name = None
        self._lambda_region = None
        if self.processor_mode == 'lambda':
            self._lambda_function_name = os.getenv('CAE_LAMBDA_FUNCTION_NAME')
            self._lambda_region = os.getenv('AWS_REGION', 'us-east-1')
            if not self._lambda_function_name:
                raise StatusException(
                    StatusException.INVALID,
                    'CAE_LAMBDA_FUNCTION_NAME environment variable is required when CAE_PROCESSOR_MODE=lambda'
                )


    def argument_validation(self, data):
        """
        Validate the arguments passed to the processor.
        """

        token = data.get('token', None)
        debug = data.get('debug', False)

        if token is None or token != os.getenv("INT_API_TOKEN", "token"):
            raise StatusException(StatusException.DENIED, 'ACCESS DENIED: wrong token')
            
        if type(debug) is not bool:
            raise StatusException(StatusException.INVALID, 'debug must be a boolean')
        if debug:
            set_log_debug()


    def _get_lambda_client(self):
        """
        Get or create boto3 Lambda client (lazy initialization).
        """
        if self._lambda_client is None:
            try:
                import boto3
            except ImportError:
                raise StatusException(
                    StatusException.ERROR,
                    'boto3 is required for Lambda mode. Install it with: pip install boto3'
                )
            self._lambda_client = boto3.client('lambda', region_name=self._lambda_region)
        return self._lambda_client


    def _invoke_lambda(self, data):
        """
        Invoke Lambda function synchronously and return normalized response.
        """
        client = self._get_lambda_client()
        
        try:
            # Prepare payload
            payload = json.dumps(data)
            Logger.debug(f'Invoking Lambda function: {self._lambda_function_name}')
            
            # Invoke synchronously
            response = client.invoke(
                FunctionName=self._lambda_function_name,
                InvocationType='RequestResponse',
                Payload=payload
            )
            
            # Parse response
            if response['StatusCode'] != 200:
                raise StatusException(
                    StatusException.ERROR,
                    f'Lambda returned status code {response["StatusCode"]}'
                )
            
            # Extract FunctionResult
            result_payload = json.load(response['Payload'])
            Logger.debug(f'Lambda response: {result_payload}')
            
            # Handle Lambda response format: {statusCode, body: {result: ...}}
            if isinstance(result_payload, dict):
                if 'body' in result_payload and isinstance(result_payload['body'], dict):
                    return result_payload['body'].get('result', result_payload)
                elif 'result' in result_payload:
                    return result_payload['result']
                else:
                    return result_payload
            else:
                return result_payload
                
        except Exception as err:
            if isinstance(err, StatusException):
                raise
            raise StatusException(
                StatusException.ERROR,
                f'Lambda invocation failed: {str(err)}'
            )


    def _normalize_output(self, outputs):
        """
        Normalize output from either local or Lambda execution to consistent format.
        """
        # If output is GeoDataFrame, convert to FeatureCollection
        if isinstance(outputs, gpd.GeoDataFrame):
            retriever = _CAERetriever()
            outputs = retriever.data_to_feature_collection(outputs)
        
        return outputs


    def execute(self, data):

        mimetype = 'application/json'
        outputs = {}
        cleanup_needed = False

        try:
            
            # DOC: Args validation
            self.argument_validation(data)
            Logger.debug(f'Validated process parameters')

            # DOC: Execute based on processor mode
            if self.processor_mode == 'lambda':
                # Lambda mode: invoke external Lambda function
                Logger.debug(f'Executing in Lambda mode')
                outputs = self._invoke_lambda(data)
            else:
                # Local mode: run retriever locally (default, backward compatible)
                Logger.debug(f'Executing in local mode')
                cleanup_needed = True
                CAERetriever = _CAERetriever()
                outputs = CAERetriever.run(**data)
                outputs = self._normalize_output(outputs)

        except StatusException as err:
            outputs = {
                'status': err.status,
                'message': str(err)
            }
        except Exception as err:
            outputs = {
                'status': StatusException.ERROR,
                'error': str(err)
            }
            raise ProcessorExecuteError(str(err))
        finally:
            # Clean up temporary data folder only in local mode
            if cleanup_needed:
                filesystem.garbage_folders(self._tmp_data_folder)
                Logger.debug(f'Cleaned up temporary data folder: {self._tmp_data_folder}')
        
        return mimetype, outputs


    def __repr__(self):
        return f'<CAERetrieverProcessor> {self.name}'