#!/usr/bin/env python3.5
from ripe.atlas.cousteau import AtlasSource, Ping, Dns, AtlasCreateRequest, AtlasResultsRequest, Measurement, MeasurementRequest
from ripe.atlas.sagan import PingResult, DnsResult
import time
import tldextract as tld
import pickle
import configparser
import sys

# Internal Libraries
import measurements

# Logger
import logging
logger = logging.getLogger('main_logger')

def format_results_for_testbed(results):
    formatted_results = dict()
    for req_id, responses in results.items():
        # Note: This loop body is taken from Arunesh's code
        # Create the local per subdomain dict
        probe_dict = dict()
        subdomain_dict = dict()
        i = 1

        # Setup variables
        measurement = Measurement(id=req_id)
        if measurement.type == 'ping':
            subdomain = measurement.target
            measurement_class = PingResult
        else:
            subdomain = measurement.meta_data['query_argument'].strip('.')
            measurement_class = DnsResult
            if measurement.type != 'dns':
                logger.error('Some error here')

        for resp in responses:
            logger.debug('Data Frame ' + str(resp))
            probe_id = resp["prb_id"]
            probe_dict[probe_id] = measurement_class(resp)
            logger.debug("Iteration Num- " + str(i))
            i += 1
        subdomain_dict[subdomain] = probe_dict
        domain_name = tld.extract(subdomain).domain
        if domain_name not in formatted_results:
            formatted_results[domain_name] = []
        formatted_results[domain_name].append(subdomain_dict)

    return formatted_results

def main(config_file = 'config/default_config.cfg'):
    # Parse Config File
    config_defaults = {
      'main': {
         'api_key': '',
         'domains_file': 'config/default_domains_list.txt'
      },
      'probe': {
        'requested': 1,
        'type': 'country',
        'value': 'US'
      }
    }
    config = configparser.ConfigParser(defaults= config_defaults)
    config.read(config_file)
    api_key = config.get('main', 'api_key')
    domains_file = config.get('main', 'domains_file')
    probe_type = config.get('probe', 'type')
    probe_value = config.get('probe', 'value')
    probe_requested = config.get('probe', 'requested')
    probe_tags = {'include': ['system-ipv4-works']}

    # Constants
    # Todo: Consider moving these to the config file
    retrieve_measurements_timeout = 5   # Seconds
    ping_request_id_file = 'ping_request_ids.txt'
    dns_request_id_file = 'dns_request_ids.txt'
    output_ping_file = 'ping_data'
    output_dns_file = 'dns_data'

    # Get domains list from a file
    domains = []
    logger.info('Reading domains file')
    with open(domains_file) as df:
        for line in df:
            logger.debug(line.strip())
            domains.append(line.strip())
    logger.debug('Domains:')
    logger.debug(domains)

    # Create the probe source
    probe_source = [AtlasSource(type=probe_type, value=probe_value, requested=int(probe_requested), tags=probe_tags)]

    ## Get the data
    logger.info('Getting Data')

    # Create ping measurements
    logger.info('Creating ping measurements')
    ping_measurements = [Ping(af=4, target=domain, description='Ping to ' + domain) for domain in domains]
    ping_success, final_ping_request_ids = measurements.create_measurements(api_key, ping_measurements, probe_source)
    logger.debug('Create ping measurement success: ' + str(ping_success))
    logger.debug('Ping results: ' + str(final_ping_request_ids))
    if not ping_success:
        logger.error('Error when creating ping measurements')
        return None
    # Get the results
    logger.info('Retrieving ping measurements')
    ping_results = measurements.retrieve_measurement_results(final_ping_request_ids, retrieve_measurements_timeout)
    # Format the output
    logger.info('Formatting ping results')
    formatted_ping_results = format_results_for_testbed(ping_results)
    # Store the formatted results to a file
    logger.info('Storing formatted ping results')
    ping_file = open(output_ping_file, 'wb+')
    ping_pickler = pickle.Pickler(ping_file, -1)
    ping_pickler.dump(formatted_ping_results)
    ping_file.close()
    logger.info('Formatted ping results')

    # Create subsequent dns measurements
    logger.info('Creating dns measurements')
    dns_measurements = [Dns(af=4, query_class='IN', query_argument=domain, query_type='A', use_probe_resolver=True,
                               include_abuf=True, retry=5, description='DNS A request for ' + domain) for domain in domains]
    dns_success, final_dns_request_ids = measurements.create_measurements(api_key, dns_measurements, probe_source)
    logger.debug('Create dns measurement success: ' + str(dns_success))
    logger.debug('Dns results: ' + str(final_dns_request_ids))
    if not dns_success:
        logger.error('Error when creating dns measurements')
        return None
    # Get the results
    logger.info('Retrieving dns measurements')
    dns_results = measurements.retrieve_measurement_results(final_dns_request_ids, retrieve_measurements_timeout)

    # Write all final ping request ids to file
    logger.info('Storing ping ids')
    ping_req_id_file = open(ping_request_id_file, 'w+')
    for req_id in final_ping_request_ids:
        ping_req_id_file.write('%s \n' % req_id)
    ping_req_id_file.close()
    # Write all final dns request ids to file
    logger.info('Storing dns ids')
    dns_req_id_file = open(dns_request_id_file, 'w+')
    for req_id in final_dns_request_ids:
        dns_req_id_file.write('%s \n' % req_id)
    dns_req_id_file.close()

    # Format the output
    logger.info('Formatting dns results')
    formatted_dns_results = format_results_for_testbed(dns_results)
    # Store the formatted results to a file
    logger.info('Storing formatted dns results')
    dns_file = open(output_dns_file, 'wb+')
    dns_pickler = pickle.Pickler(dns_file, -1)
    dns_pickler.dump(formatted_dns_results)
    dns_file.close()
    logger.info('Formatted dns results')

if __name__ == '__main__':
    # Setup the logger
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(fmt='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
                                    datefmt='%m/%d/%Y %I:%M:%S')
    # Used to print to console
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    # Used to output to a file
    log_file = 'main.log'
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    logger.addHandler(ch)
    logger.addHandler(fh)

    # Get config argument
    config_arg = sys.argv[1]
    # Run the script
    main(config_arg)
