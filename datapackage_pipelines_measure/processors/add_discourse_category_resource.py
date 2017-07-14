import collections

from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew

from datapackage_pipelines_measure.processors.discourse_utils import (
    request_data_from_discourse,
    request_report_from_discourse
)

import logging
log = logging.getLogger(__name__)


def _get_category_id_from_category_name(domain, category):
    '''This isn't a great way to get the category id...'''
    endpoint = "/c/{}.json".format(category)
    category_data = request_data_from_discourse(domain, endpoint,
                                                no_subcategories='true')
    return category_data['topic_list']['topics'][0]['category_id']


def discourse_collector(domain, category, child_treatment, latest_row):
    latest_date = latest_row['date'] if latest_row else None
    category_id = _get_category_id_from_category_name(domain, category)
    new_topics_by_date = request_report_from_discourse(domain, 'topics',
                                                       latest_date,
                                                       category_id=category_id)
    new_posts_by_date = request_report_from_discourse(domain, 'posts',
                                                      latest_date,
                                                      category_id=category_id)

    dd = collections.defaultdict(lambda: {'new_topics': 0,
                                          'new_posts': 0})

    for date, topic_num in new_topics_by_date.items():
        dd[date]['new_topics'] = topic_num

    for date, post_num in new_posts_by_date.items():
        dd[date]['new_posts'] = post_num

    resource_content = []
    for date, stats in dd.items():
        res_row = {
            'source': 'discourse',
            'domain': domain,
            'category': category,
            'date': date,
            'new_topics': stats['new_topics'],
            'new_posts': stats['new_posts']
        }
        resource_content.append(res_row)

    return resource_content


parameters, datapackage, res_iter = ingest()

domain = parameters['domain']
category = parameters['category']['name']
child_treatment = parameters['category']['children']

resource = {
    'name': slugify(domain),
    'path': 'data/{}.csv'.format(slugify(domain))
}

headers = ['domain', 'category', 'source', 'date', 'new_topics', 'new_posts']
resource['schema'] = {'fields': [{'name': h, 'type': 'string'}
                                 for h in headers]}

datapackage['resources'].append(resource)


def process_resources(res_iter, datapackage, domain,
                      category, child_treatment):

    def get_latest_row(first):
        latest_row = None
        my_rows = []
        for row in first:
            if row['domain'] == domain and row['source'] == 'discourse':
                latest_row = row
            my_rows.append(row)
        return latest_row, iter(my_rows)

    if len(datapackage['resources']):
        if datapackage['resources'][0]['name'] == 'latest-project-entries':
            latest_row, latest_iter = get_latest_row(next(res_iter))
            yield latest_iter
        else:
            latest_row = None
    yield from res_iter
    yield discourse_collector(domain, category, child_treatment, latest_row)


spew(datapackage, process_resources(res_iter, datapackage, domain,
                                    category, child_treatment))
