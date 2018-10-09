"""
varnishd 4.0
"""
import logging
import platform
import subprocess
import json

from newrelic_plugin_agent.plugins import base

LOGGER = logging.getLogger(__name__)


class Varnishd(base.Plugin):

    GUID = 'com.meetme.newrelic_varnishd_agent'

    KEYS = ['client_req',
            'backend_fail',
            'cache_miss',
            'cache_hit',
            'threads',
            'threads_created',
            'threads_failed',
            'threads_limited',
            'sess_drop',
            'sess_conn',
            'sess_fail',
            'n_lru_nuked',
            'esi_errors',
            'n_expired']

    def add_datapoints(self, stats):
        """Add all of the data points for a node

        :param dict stats: all of the nodes

        """
        # the default varnish instance name is local hostname
        instance_name = self.config.get('instance', platform.node())
        newrelic_name = self.config.get('newrelic_instance', instance_name)
        base_name = 'Varnish/%s' % newrelic_name
        self.add_derive_value('%s/Requests/received' % base_name, 'client_req',
                              stats['client_req'])
        self.add_derive_value('%s/Backend/failures' % base_name, 'backend_fail',
                              stats['backend_fail'])
        self.add_derive_value('%s/Cache/misses' % base_name, 'cache_miss',
                              stats['cache_miss'])
        self.add_derive_value('%s/Cache/hits' % base_name, 'cache_hit',
                              stats['cache_hit'])
        self.add_derive_value('%s/Threads/total' % base_name, 'threads',
                              stats['threads'])
        self.add_derive_value('%s/Threads/created' % base_name, 'threads_created',
                              stats['threads_created'])
        self.add_derive_value('%s/Threads/failed' % base_name, 'threads_failed',
                              stats['threads_failed'])
        self.add_derive_value('%s/Threads/limited' % base_name, 'threads_limited',
                              stats['threads_limited'])
        self.add_derive_value('%s/Sessions/accepted' % base_name, 'sess_conn',
                              stats['sess_conn'])
        self.add_derive_value('%s/Sessions/failed' % base_name, 'sess_fail',
                              stats['sess_fail'])
        self.add_derive_value('%s/Sessions/dropped' % base_name, 'sess_drop',
                              stats['sess_drop'])
        self.add_derive_value('%s/LRU/nuked' % base_name, 'n_lru_nuked',
                              stats['n_lru_nuked'])
        self.add_derive_value('%s/esi/errors' % base_name, 'esi_errors',
                              stats['esi_errors'])
        self.add_derive_value('%s/Objects/expired' % base_name, 'n_expired',
                              stats['n_expired'])

    def fetch_data(self):
        """Fetch the data from varnish stats command
        :rtype: dict

        """
        # the default varnish instance name is local hostname
        instance = self.config.get('instance', platform.node())
        varnishstat = self.config.get('varnishstat', 'varnishstat')
        try:
            p = subprocess.Popen(
                [varnishstat, "-1", "-j", "-n", instance], stdout=subprocess.PIPE)
            stdout, err = p.communicate()
            return json.loads(stdout)
        except Exception as error:
            LOGGER.error('Subprocess error: %r', error)
        return {}

    def parse_metrics(self, data):
        """
        filters the appropriate metric types from metric KEYS
        :rtype: dict
        """
        parsed_data = {}
        try:
            for metricname in self.KEYS:
                jsonmetric = "MAIN." + metricname
                metric = data[jsonmetric]['value']
                parsed_data[metricname] = metric
        except Exception as error:
            LOGGER.error('Metrics parsing error: %r', error)
        return parsed_data

    def poll(self):
        """Poll subprocess for stats data"""
        self.initialize()
        data = self.fetch_data()
        parsed_data = self.parse_metrics(data)
        if parsed_data:
            self.add_datapoints(parsed_data)
        self.finish()
