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

        'out_format': {
            'title': 'Return format type',
            'description': 'The return format type. Possible values are "geojson" or "dataframe". "geojson" is default and preferable.',
            'schema': {
            }
        }, 

        'bucket_destination': {
            'title': 'Bucket destination',
            'description': 'The bucket destination where the data will be stored. If not provided, the data will not be stored in a bucket.',
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

        # REF: https://arpaebo.caedns.it/datascape/api-doc/index.html (Swagger API Documentation)
        self.base_urls = 'https://arpaebo.caedns.it/datascape'
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


    def argument_validation(self, data):
        """
        Validate the arguments passed to the processor.
        """

        token = data.get('token', None)
        lat_range = data.get('lat_range', None)
        long_range = data.get('long_range', None)
        time_range = data.get('time_range', None)
        time_start = time_range[0] if type(time_range) in [list, tuple] else time_range
        time_end = time_range[1] if type(time_range) in [list, tuple] else None
        filters = data.get('filters', None)
        out_format = data.get('out_format', None)
        bucket_destination = data.get('bucket_destination', None)
        debug = data.get('debug', False)

        if token is None or token != os.getenv("INT_API_TOKEN", "token"):
            raise StatusException(StatusException.DENIED, 'ACCESS DENIED: wrong token')
        
        if lat_range is None:
            raise StatusException(StatusException.INVALID, 'Cannot process without a lat_range')
        if type(lat_range) is not list or len(lat_range) != 2:
            raise StatusException(StatusException.INVALID, 'lat_range must be a list of 2 elements')
        if type(lat_range[0]) not in [int, float] or type(lat_range[1]) not in [int, float]:
            raise StatusException(StatusException.INVALID, 'lat_range elements must be float')
        if lat_range[0] < -90 or lat_range[0] > 90 or lat_range[1] < -90 or lat_range[1] > 90:
            raise StatusException(StatusException.INVALID, 'lat_range elements must be in the range [-90, 90]')
        if lat_range[0] > lat_range[1]:
            raise StatusException(StatusException.INVALID, 'lat_range[0] must be less than lat_range[1]')
        
        if long_range is None:
            raise StatusException(StatusException.INVALID, 'Cannot process without a long_range')
        if type(long_range) is not list or len(long_range) != 2:
            raise StatusException(StatusException.INVALID, 'long_range must be a list of 2 elements')
        if type(long_range[0]) not in [int, float] or type(long_range[1]) not in [int, float]:
            raise StatusException(StatusException.INVALID, 'long_range elements must be float')
        if long_range[0] < -180 or long_range[0] > 180 or long_range[1] < -180 or long_range[1] > 180:
            raise StatusException(StatusException.INVALID, 'long_range elements must be in the range [-180, 180]')
        if long_range[0] > long_range[1]:
            raise StatusException(StatusException.INVALID, 'long_range[0] must be less than long_range[1]')
        
        if time_start is None:
            raise StatusException(StatusException.INVALID, 'Cannot process without a time valued')
        if type(time_start) is not str:
            raise StatusException(StatusException.INVALID, 'time_start must be a string')
        if type(time_start) is str:
            try:
                time_start = datetime.datetime.fromisoformat(time_start)
            except ValueError:
                raise StatusException(StatusException.INVALID, 'time_start must be a valid datetime iso-format string')
        
        if time_end is not None:
            if type(time_end) is not str:
                raise StatusException(StatusException.INVALID, 'time_end must be a string')
            if type(time_end) is str:
                try:
                    time_end = datetime.datetime.fromisoformat(time_end)
                except ValueError:
                    raise StatusException(StatusException.INVALID, 'time_end must be a valid datetime iso-format string')
            if time_start > time_end:
                raise StatusException(StatusException.INVALID, 'time_start must be less than time_end')
            
        if filters is not None:
            if type(filters) is not dict:
                raise StatusException(StatusException.INVALID, 'filters must be a dictionary')
            for fkey, fvalue in filters.items():
                if type(fvalue) not in [str, list]:
                    raise StatusException(StatusException.INVALID, f'filter {fkey} must be a string or a list of strings')
                if type(fvalue) is str:
                    filters[fkey] = [fvalue]
                elif type(fvalue) is list:
                    for fitem in fvalue:
                        if type(fitem) is not str:
                            raise StatusException(StatusException.INVALID, f'filter {fkey} items must be strings')
            
        if type(out_format) is not str:
            raise StatusException(StatusException.INVALID, 'out_format must be a string or null')
        if out_format not in ['geojson']:
            raise StatusException(StatusException.INVALID, 'out_format must be one of ["geojson"]')
        
        if bucket_destination is not None:
            if type(bucket_destination) is not str:
                raise StatusException(StatusException.INVALID, 'bucket_destination must be a string')
            if not bucket_destination.startswith('s3://'):
                raise StatusException(StatusException.INVALID, 'bucket_destination must start with "s3://"')
            
        if type(debug) is not bool:
            raise StatusException(StatusException.INVALID, 'debug must be a boolean')
        if debug:
            set_log_debug()

        return {
            'token': token,
            'lat_range': lat_range,
            'long_range': long_range,
            'time_start': time_start,
            'time_end': time_end,
            'filters': filters,
            'out_format': out_format,
            'bucket_destination': bucket_destination,
            'debug': debug
        }
    

    def retrieve_data(self, long_range, lat_range, time_start, time_end, filters):

        # DOC: Authenticate and get the token (if not already cached)
        auth_secret = os.path.join(self._cache_data_folder, f'{self.name}_auth.json')
        if not os.path.exists(auth_secret):
            # DOC: Authenticate with the CAE API
            payload = {
                'username': os.getenv("CAE_API_USERNAME", ""),
                'password': os.getenv("CAE_API_PASSWORD", ""),
                'grant_type': os.getenv("CAE_API_GRANT_TYPE", ""),
                'client_id': os.getenv("CAE_API_CLIENT_ID", ""),
                'client_instance': uuid.uuid4().hex
            }
            auth_response = requests.post(self.auth_url, payload, verify=False)
            if not auth_response.ok:
                raise StatusException(StatusException.ERROR, f'Error in authentication from {self.auth_url}: {auth_response.status_code} - {auth_response.text}')
            auth_data = auth_response.json()
            auth_data['created_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            auth_data['expires_at'] = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=auth_data['expires_in'])).isoformat()
            with open(auth_secret, 'w') as f:
                json.dump(auth_data, f)
            Logger.debug('CAE API authentication successful')
        else:
            with open(auth_secret, 'r') as f:
                auth_data = json.load(f)
            if datetime.datetime.now(datetime.timezone.utc) > datetime.datetime.fromisoformat(auth_data['expires_at']):
                # DOC: Refresh the token and update the auth_secret file
                raise StatusException(StatusException.ERROR, "Token expired, please re-authenticate")
            else:
                # DOC: Load the authentication data from the local file
                auth_data = json.load(open(auth_secret, 'r'))   
                Logger.debug('CAE API authentication loaded from local file')
        auth_headers =  { 'Authorization': auth_data['token_type'] + ' ' + auth_data['access_token'] }

        # DOC: Retrieve sensor informations (if not already cached)
        sensor_gdf_fn = os.path.join(self._cache_data_folder, f'{self.name}_sensors.geojson')
        if not os.path.exists(sensor_gdf_fn):
            # DOC: Retrieve sensor informations - 1 - Get the sensor list
            sensor_list_response = requests.get(self.sensor_list_url, headers=auth_headers, verify=False)
            if not sensor_list_response.ok:
                raise StatusException(StatusException.ERROR, f'Error retrieving sensor list from {self.sensor_list_url}: {sensor_list_response.status_code} - {sensor_list_response.text}')
            sensor_list_data = sensor_list_response.json()
            sensors_df = pd.DataFrame(sensor_list_data)
            # DOC: Retrieve sensor informations - 2 - Get the sensor locations
            location_response = requests.get(self.location_url, headers=auth_headers, params={'category': 'All'}, verify=False)
            if not location_response.ok:
                raise StatusException(StatusException.ERROR, f'Error retrieving sensor locations from {self.location_url}: {location_response.status_code} - {location_response.text}')
            location_data = location_response.json()
            locations_df = pd.DataFrame(location_data)[['i', 'x', 'y']].rename(columns={'i': 'stationId', 'x': 'longitude', 'y': 'latitude'})
            sensors_df = sensors_df.merge(locations_df, on='stationId', how='left')
            # DOC: Retrieve sensor informations - 3 - Get the sensor specifications
            sensors_specs = []
            for sensor_id in sensors_df['id'].unique():
                sensor_specs_response = requests.get(self.sensor_specs_url(sensor_id), headers=auth_headers, verify=False)
                if not sensor_specs_response.ok:
                    raise StatusException(StatusException.ERROR, f'Error retrieving sensor specifications from {self.sensor_specs_url(sensor_id)}: {sensor_specs_response.status_code} - {sensor_specs_response.text}')
                sensor_specs_data = sensor_specs_response.json()
                sensors_specs.append({
                    'elementId': sensor_specs_data['elementId'],
                    'instrument': sensor_specs_data['instrument'],
                    'quantity': sensor_specs_data['quantityDescrC'].lower().replace(' ', '_'),
                })
            sensors_specs_df = pd.DataFrame(sensors_specs)
            sensors_df = sensors_df.merge(sensors_specs_df, on='elementId', how='left')
            # DOC: Retrieve sensor informations - 4 - Converting to GeoDataFrame and store it
            sensors_gdf = gpd.GeoDataFrame(
                sensors_df, 
                geometry=gpd.points_from_xy(sensors_df.longitude, sensors_df.latitude),
                crs='EPSG:4326'
            )
            sensors_gdf.to_file(sensor_gdf_fn, driver='GeoJSON', index=False)
            Logger.debug(f'Retrieved {len(sensors_gdf)} sensors from CAE API')
        else:
            sensors_gdf = gpd.read_file(sensor_gdf_fn)
            Logger.debug(f'Loaded {len(sensors_gdf)} sensors from local file {sensor_gdf_fn}')

        # DOC: Filter sensors based on lat/long ranges
        if lat_range is not None:
            sensors_gdf = sensors_gdf[(sensors_gdf.geometry.y >= lat_range[0]) & (sensors_gdf.geometry.y <= lat_range[1])]
        if long_range is not None:
            sensors_gdf = sensors_gdf[(sensors_gdf.geometry.x >= long_range[0]) & (sensors_gdf.geometry.x <= long_range[1])]
        Logger.debug(f'Filtered sensors to {len(sensors_gdf)} based on lat/long ranges')

        # DOC: Filter sensors based on provided filters
        if filters is not None:
            if 'station' in filters:
                sensors_gdf = sensors_gdf[sensors_gdf['stationId'].isin(filters['station'])]
            if 'element' in filters:
                sensors_gdf = sensors_gdf[sensors_gdf['elementId'].isin(filters['element'])]
            if 'instrument' in filters:
                sensors_gdf = sensors_gdf[sensors_gdf['instrument'].isin(filters['instrument'])]
            if 'quantity' in filters:
                sensors_gdf = sensors_gdf[sensors_gdf['quantity'].isin(filters['quantity'])]
            Logger.debug(f'Filtered sensors to {len(sensors_gdf)} based on provided filters')
        
        # DOC: Retrieve sensor data
        sensors_gdf['data'] = None
        for sensor_id in sensors_gdf['id'].unique():
            params = {
                'from': time_start.replace(tzinfo=datetime.timezone.utc).isoformat(),
                ** ( {'to': time_end.replace(tzinfo=datetime.timezone.utc).isoformat() } if time_end else dict() ),
                'outUtcOffset': '+00:00',
                'part': ['IsoTime', 'Value', 'Quality']
            }
            sensor_data_response = requests.get(self.sensor_data_url, headers=auth_headers, params=params, verify=False)
            if not sensor_data_response.ok:
                raise StatusException(StatusException.ERROR, f'Error retrieving sensor data from {self.sensor_data_url(sensor_id)}: {sensor_data_response.status_code} - {sensor_data_response.text}')
            sensor_data = sensor_data_response.json()
            sensor_data_df = pd.DataFrame(sensor_data, columns=['IsoTime', 'Value', 'Quality'])
            sensor_data_df = sensor_data_df[sensor_data_df['Value'].notnull()]
            sensors_gdf.at[sensors_gdf['id'] == sensor_id, 'data'] = [(dt.replace(tzinfo=None), val) for dt,val in zip(sensor_data_df['IsoTime'], sensor_data_df['Value'])]
            Logger.debug(f'Retrieved {len(sensor_data_df)} data points for sensor {sensor_id}')

        return sensors_gdf


    def data_to_feature_collection(self, sensors_gdf):
        """
        Convert the sensors GeoDataFrame to a GeoJSON FeatureCollection.
        """

        def build_metadata():
            variable_metadata = [
                {
                    '@name': 'water_level',
                    '@alias': 'water_level',
                    '@unit': 'm',
                    '@type': 'level'
                },
                {
                    '@name': 'accumulated_rainfall',
                    '@alias': 'accumulated_rainfall',
                    '@unit': 'mm',
                    '@type': 'rainfall'
                },
                {
                    '@name': 'rainfall_increment',
                    '@alias': 'rainfall_increment',
                    '@unit': 'mm',
                    '@type': 'rainfall'
                },
                {
                    '@name': 'air_temperature',
                    '@alias': 'air_temperature',
                    '@unit': '°C',
                    '@type': 'temperature'
                }
            ]
            variable_metadata
            return variable_metadata
            
        def build_crs():
            return {
                "type": "name",
                "properties": {
                    "name": "urn:ogc:def:crs:OGC:1.3:CRS84"  # REF: https://gist.github.com/sgillies/1233327 lines 256:271
                }
            }

        features = []
        for _, row in sensors_gdf.iterrows():
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [row.geometry.x, row.geometry.y]
                },
                'properties': {
                    'id': row['elementId'],
                    'element_id': row['elementId'],
                    'element_name': row['elementName'],
                    'station_id': row['stationId'],
                    'station_name': row['stationName'],
                    'um': row['um'],
                    'instrument': row['instrument'],
                    row['quantity']: [(dt.isoformat(), val) for dt,val in row['data']]
                }
            }
            features.append(feature)
        
        feature_collection = {
            'type': 'FeatureCollection',
            'features': features,
            'metadata': {
                'field': build_metadata(),
            },
            'crs': build_crs()
        }

        return feature_collection



    def execute(self, data):

        mimetype = 'application/json'

        outputs = {}

        try:
            
            # DOC: Args validation
            validated_data = self.argument_validation(data)
            token = validated_data['token']
            lat_range = validated_data['lat_range']
            long_range = validated_data['long_range']
            time_start = validated_data['time_start']
            time_end = validated_data['time_end']
            filters = validated_data['filters']
            out_format = validated_data['out_format']
            bucket_destination = validated_data['bucket_destination']
            debug = validated_data['debug']
            Logger.debug(f"Validated data: {validated_data}")

            # DOC: Retrieve data
            sensors_gdf = self.retrieve_data(long_range, lat_range, time_start, time_end, filters)

            # DOC: Build feature collection
            feature_collection = self.data_to_feature_collection(sensors_gdf)
            feature_collection_fn = f'{self.name}__{time_start.isoformat()}__{time_end.isoformat() if time_end else datetime.datetime.now(tz=datetime.timezone.utc).isoformat()}.geojson'
            feature_collection_fp = os.path.join(self._tmp_data_folder, feature_collection_fn)
            with open(feature_collection_fp, 'w') as f:
                json.dump(feature_collection, f)
            Logger.debug(f"Feature collection saved to {feature_collection_fp}")

            # DOC: Store data in bucket if bucket_destination is provided
            if bucket_destination is not None:
                bucket_path = os.path.join(bucket_destination, feature_collection_fn)
                module_s3.s3_upload(feature_collection_fn, bucket_path)
                Logger.debug(f"Data stored in bucket: {bucket_path}")

            # DOC: Prepare outputs
            if out_format == 'geojson':
                outputs = feature_collection
            else:
                if bucket_destination is not None:
                    outputs = {
                        'status': 'OK',
                        'uri': bucket_path,
                    }
                else:
                    outputs = {
                        'status': 'OK',
                        'filepath': feature_collection_fp
                    }

            Logger.debug(f"Outputs prepared")

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
        
        filesystem.garbage_folders(self._tmp_data_folder)
        Logger.debug(f'Cleaned up temporary data folder: {self._tmp_data_folder}')
        
        return mimetype, outputs


    def __repr__(self):
        return f'<CAERetrieverProcessor> {self.name}'