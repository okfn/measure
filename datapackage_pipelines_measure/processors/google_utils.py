import httplib2

# google api client
from apiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials

from datapackage_pipelines_measure.config import settings

GOOGLE_API_BIGQUERY_DEFAULT_TIMEOUT_MILLISECONDS = 60000
GOOGLE_API_BIGQUERY_VERSION = 'v2'
GOOGLE_API_BIGQUERY_SERVICE_NAME = 'bigquery'
GOOGLE_API_BIGQUERY_SCOPES = \
    'https://www.googleapis.com/auth/bigquery'
JWT_NAMESPACE = 'GOOGLE_API_JWT_'


def get_google_api_service(service_name, service_version, scopes):
    '''Use Google Client API to get a Google API Service.

    :param service_name: the name of requested service, e.g. 'sheets'
    :param service_version: the version of requested service, e.g. 'v4'
    :param scopes: the authentication requested.

    :return: googleapiclient.discovery.Resource Object, tied to google
    service API.
    '''
    def _build_jwt_dict():
        jwt_dict = {key.replace(JWT_NAMESPACE, '').lower(): settings[key]
                    for key in settings
                    if key.startswith(JWT_NAMESPACE)}
        jwt_dict['PROJECT_ID'] = settings['GOOGLE_API_PROJECT_ID']
        return jwt_dict

    def _get_authorized_http_object(scopes):
        '''JWT credentials authorization.

        :param scopes: the authorization scope to be requested
        :return: httplib2.Http object, with authorized credentials
        '''
        credentials = ServiceAccountCredentials.from_json_keyfile_dict(
            _build_jwt_dict(), scopes=scopes)
        return credentials.authorize(httplib2.Http())

    try:
        return discovery.build(
            service_name,
            service_version,
            http=_get_authorized_http_object(scopes)
        )
    except AttributeError:  # config variables are missing
        # todo: write as real exception
        raise
