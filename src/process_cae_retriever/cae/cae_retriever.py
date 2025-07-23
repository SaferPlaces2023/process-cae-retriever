import os
import json
import uuid
import datetime
import urllib3
import requests

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point

from ..cli.module_log import Logger
from ..utils import filesystem, module_s3
from ..utils.status_exception import StatusException


urllib3.disable_warnings()


class _CAERetriever():
    """
    Class to retrieve data from CAE sensors.
    """

    def __init__(self):
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


    def argument_validation(self, **kwargs):
        """
        Validate the arguments passed to the processor.
        """

        lat_range = kwargs.get('lat_range', None)
        long_range = kwargs.get('long_range', None)
        time_range = kwargs.get('time_range', None)
        time_start = time_range[0] if type(time_range) in [list, tuple] else time_range
        time_end = time_range[1] if type(time_range) in [list, tuple] else None
        filters = kwargs.get('filters', None)
        out_format = kwargs.get('out_format', None)
        bucket_destination = kwargs.get('bucket_destination', None)
        out = kwargs.get('out', None)

        if lat_range is not None:
            if type(lat_range) is not list or len(lat_range) != 2:
                raise StatusException(StatusException.INVALID, 'lat_range must be a list of 2 elements')
            if type(lat_range[0]) not in [int, float] or type(lat_range[1]) not in [int, float]:
                raise StatusException(StatusException.INVALID, 'lat_range elements must be float')
            if lat_range[0] < -90 or lat_range[0] > 90 or lat_range[1] < -90 or lat_range[1] > 90:
                raise StatusException(StatusException.INVALID, 'lat_range elements must be in the range [-90, 90]')
            if lat_range[0] > lat_range[1]:
                raise StatusException(StatusException.INVALID, 'lat_range[0] must be less than lat_range[1]')
        
        if long_range is not None:
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

        if out_format is not None:  
            if type(out_format) is not str:
                raise StatusException(StatusException.INVALID, 'out_format must be a string or null')
            if out_format not in ['geojson']:
                raise StatusException(StatusException.INVALID, 'out_format must be one of ["geojson"]')
        else:
            out_format = 'geojson'
        
        if bucket_destination is not None:
            if type(bucket_destination) is not str:
                raise StatusException(StatusException.INVALID, 'bucket_destination must be a string')
            if not bucket_destination.startswith('s3://'):
                raise StatusException(StatusException.INVALID, 'bucket_destination must start with "s3://"')
            
        if out is not None:
            if type(out) is not str:
                raise StatusException(StatusException.INVALID, 'out must be a string')
            if not out.endswith('.geojson'):
                raise StatusException(StatusException.INVALID, 'out must end with ".geojson"')
            dirname, _ = os.path.split(out)
            if dirname != '' and not os.path.exists(dirname):
                os.makedirs(dirname)

        return {
            'lat_range': lat_range,
            'long_range': long_range,
            'time_start': time_start,
            'time_end': time_end,
            'filters': filters,
            'out_format': out_format,
            'bucket_destination': bucket_destination,
            'out': out
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
            for sensor_id in sensors_df['elementId'].unique():
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
        sensors_gdf['data'] = sensors_gdf['data'].astype(object)
        for r_idx, row in sensors_gdf.iterrows():
            sensor_id = row['elementId']
            params = {
                'from': time_start.replace(tzinfo=datetime.timezone.utc).isoformat(),
                ** ( {'to': time_end.replace(tzinfo=datetime.timezone.utc).isoformat() } if time_end else dict() ),
                'outUtcOffset': '+00:00',
                'part': ['IsoTime', 'Value', 'Quality']
            }
            sensor_data_response = requests.get(self.sensor_data_url(sensor_id), headers=auth_headers, params=params, verify=False)
            if not sensor_data_response.ok:
                raise StatusException(StatusException.ERROR, f'Error retrieving sensor data from {self.sensor_data_url(sensor_id)}: {sensor_data_response.status_code} - {sensor_data_response.text}')
            sensor_data = sensor_data_response.json()
            sensor_data_df = pd.DataFrame(sensor_data, columns=['IsoTime', 'Value', 'Quality'])
            sensor_data_df = sensor_data_df[sensor_data_df['Value'].notnull()]
            sensors_gdf.at[r_idx, 'data'] = [(datetime.datetime.fromisoformat(dt).replace(tzinfo=None), val) for dt,val in zip(sensor_data_df['IsoTime'], sensor_data_df['Value'])]
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
                    'um': row['measUnit'],
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
    

    def run(
        self,
        lat_range = None,
        long_range = None,
        time_range = None,
        filters = None,
        out_format = None,
        bucket_destination = None,
        out = None,
        **kwargs
    ):
        
        """
        Run the CAE Retriever.
        """

        try:

            # DOC: Validate the arguments
            validated_args = self.argument_validation(
                lat_range=lat_range,
                long_range=long_range,
                time_range=time_range,
                filters=filters,
                out=out,
                out_format=out_format,
                bucket_destination=bucket_destination,
            )
            lat_range = validated_args['lat_range']
            long_range = validated_args['long_range']
            time_start = validated_args['time_start']
            time_end = validated_args['time_end']
            filters = validated_args['filters']
            out_format = validated_args['out_format']
            out = validated_args['out']
            bucket_destination = validated_args['bucket_destination']
            Logger.debug(f"Running CAE Retriever with parameters: {validated_args}")

            # DOC: Retrieve data from CAE API
            sensors_gdf = self.retrieve_data(
                long_range=long_range,
                lat_range=lat_range,
                time_start=time_start,
                time_end=time_end,
                filters=filters
            )
            Logger.debug(f"Retrieved {len(sensors_gdf)} sensors data from CAE API")

            # DOC: Build feature collection
            if out_format == 'geojson':
                feature_collection = self.data_to_feature_collection(sensors_gdf)
                feature_collection_fn = filesystem.normpath(f'{self.name}__{time_start.isoformat()}__{time_end.isoformat() if time_end else datetime.datetime.now(tz=datetime.timezone.utc).isoformat()}.geojson')
                feature_collection_fp = os.path.join(self._tmp_data_folder, feature_collection_fn) if out is None else out
                with open(feature_collection_fp, 'w') as f:
                    json.dump(feature_collection, f)
                output_filespaths = [feature_collection_fp]
                Logger.debug(f"Feature collection saved to {feature_collection_fp}")

            # DOC: Store data in bucket if bucket_destination is provided
            if bucket_destination is not None:
                bucket_uris = []
                for output_filepath in output_filespaths:
                    output_filename = os.path.basename(output_filepath)
                    bucket_uri = f'{bucket_destination}/{output_filename}'
                    upload_status = module_s3.s3_upload(output_filename, bucket_uri)
                    if not upload_status:
                        raise StatusException(StatusException.ERROR, f"Failed to upload data to bucket {bucket_destination}")
                    bucket_uris.append(bucket_uri)
                    Logger.debug(f"Data stored in bucket: {bucket_uri}")

            # DOC: Prepare outputs
            if bucket_destination is not None or out is not None:
                outputs = { 'status': 'OK' }
                if bucket_destination is not None:
                    outputs = {
                        ** outputs,
                        ** ( {'uri': bucket_uri[0]} if len(bucket_uris) == 1 else {'uris': bucket_uris} )
                    }
                if out is not None:
                    outputs = {
                        ** outputs,
                        ** ( {'filepath': output_filespaths[0]} if len(output_filespaths) == 1 else {'filepaths': output_filespaths} )
                    }
            else:
                outputs = sensors_gdf
            Logger.debug(f"Outputs prepared")

            # DOC: Clean up temporary data folder
            filesystem.garbage_folders(self._tmp_data_folder)
            Logger.debug(f'Cleaned up temporary data folder: {self._tmp_data_folder}')

            return outputs
    
        except Exception as ex:
            # DOC: Clean up temporary data folder and forward exception
            filesystem.garbage_folders(self._tmp_data_folder)
            Logger.debug(f'Cleaned up temporary data folder: {self._tmp_data_folder} before raising exception')
            raise ex