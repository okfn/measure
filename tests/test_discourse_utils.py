import mock
import pytest
import datapackage_pipelines_measure.processors.discourse_utils as discourse_utils
from datapackage_pipelines_measure.config import settings


class TestDiscourseUtilsRequestDataFromDiscourse(object):
    def test_returns_the_data_as_dict(self, requests_mock):
        domain = 'example.com'
        endpoint = '/endpoint'
        expected_json_response = {'foo': 'bar'}
        requests_mock.get('https://example.com/endpoint', json=expected_json_response)

        response = discourse_utils.request_data_from_discourse(domain, endpoint)

        assert response == expected_json_response

    def test_raises_if_response_wasnt_successful(self, requests_mock):
        domain = 'example.com'
        endpoint = '/endpoint'
        expected_error_message = 'some error message'

        requests_mock.get(
            'https://example.com/endpoint',
            status_code=500,
            text=expected_error_message
        )

        with pytest.raises(ValueError) as e:
            discourse_utils.request_data_from_discourse(domain, endpoint)

        assert expected_error_message in str(e)

    def test_retries_responses_429_after_waiting_some_time(self, requests_mock):
        domain = 'example.com'
        endpoint = '/endpoint'
        expected_json_response = {'foo': 'bar'}

        requests_mock.get(
            'https://example.com/endpoint',
            [
                {'status_code': 429},
                {'status_code': 200, 'json': expected_json_response}
            ],
        )

        with mock.patch('time.sleep', return_value=None) as sleep_mock:
            response = discourse_utils.request_data_from_discourse(domain, endpoint)

        assert response == expected_json_response
        sleep_mock.assert_called()


@pytest.fixture
def requests_mock():
    import requests
    import requests_mock

    with requests_mock.mock() as m:
        yield m
