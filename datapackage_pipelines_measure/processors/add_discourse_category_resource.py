import collections

from datapackage_pipelines.generators import slugify
from datapackage_pipelines.wrapper import ingest, spew
from datapackage_pipelines.utilities.resources import PROP_STREAMING

from datapackage_pipelines_measure.processors.discourse_utils import (
    request_report_from_discourse,
    get_category_info_from_discourse
)

import logging
log = logging.getLogger(__name__)


def _get_category_id_from_category_name(domain, category):
    category_map = get_category_info_from_discourse(domain)
    return next(c['id'] for c in category_map if c['slug'] == category)


def _get_category_child_ids(domain, category_id):
    '''Given a `category_id` return a list of subcategory ids.'''
    category_map = get_category_info_from_discourse(domain)
    parent_category = next(c for c in category_map if c['id'] == category_id)
    return parent_category['subcategories']


def discourse_collector(domain, category, child_treatment, latest_row):  # noqa (ignore mccabe for this function)
    def _make_stats_dict_for_category_id(category_id, category_name):
        '''Return a defaultdict where items have a date as a key, and category
        stats for a value, for a given category_id and category_name'''
        dd = collections.defaultdict(lambda: {'new_topics': 0,
                                              'new_posts': 0,
                                              'category': category_name})
        new_topics_by_date = \
            request_report_from_discourse(domain, 'topics', latest_date,
                                          category_id=category_id)
        new_posts_by_date = \
            request_report_from_discourse(domain, 'posts', latest_date,
                                          category_id=category_id)

        for date, topic_num in new_topics_by_date.items():
            dd[date]['new_topics'] = topic_num

        for date, post_num in new_posts_by_date.items():
            dd[date]['new_posts'] = post_num

        return dd

    latest_date = latest_row['date'] if latest_row else None
    category_id = _get_category_id_from_category_name(domain, category)

    toplevel_dd = _make_stats_dict_for_category_id(category_id, category)

    row_data_to_chain = [toplevel_dd]

    if child_treatment == 'aggregate':
        # Add child category data to parent data
        child_categories = _get_category_child_ids(domain, category_id)
        for child in child_categories:
            child_new_topics_by_date = \
                request_report_from_discourse(domain, 'topics', latest_date,
                                              category_id=child['id'])
            child_new_posts_by_date = \
                request_report_from_discourse(domain, 'posts', latest_date,
                                              category_id=child['id'])
            for date, topic_num in child_new_topics_by_date.items():
                toplevel_dd[date]['new_topics'] += topic_num
            for date, post_num in child_new_posts_by_date.items():
                toplevel_dd[date]['new_posts'] += post_num
    elif child_treatment == 'expand':
        # Save child category data as separate rows
        child_categories = _get_category_child_ids(domain, category_id)
        for child in child_categories:
            child_dd = _make_stats_dict_for_category_id(child['id'],
                                                        child['slug'])
            row_data_to_chain.append(child_dd)

    resource_content = []
    for dd in row_data_to_chain:
        for date, stats in dd.items():
            res_row = {
                'source': 'discourse',
                'domain': domain,
                'category': stats['category'],
                'date': date,
                'new_topics': stats['new_topics'],
                'new_posts': stats['new_posts']
            }
            resource_content.append(res_row)

    return resource_content


parameters, datapackage, res_iter = ingest()

domain = parameters['domain']
category = parameters['category']['name']
child_treatment = parameters['category'].get('children', None)

resource = {
    'name': slugify(domain),
    'path': 'data/{}.csv'.format(slugify(domain)),
    PROP_STREAMING: True
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
            if row['domain'] == domain and row['category'] == category \
               and row['source'] == 'discourse':
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
