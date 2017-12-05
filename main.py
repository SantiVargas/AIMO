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
        'country_code': 'US'
      }
    }
    config = configparser.ConfigParser(defaults= config_defaults)
    config.read(config_file)
    api_key = config.get('main', 'api_key')
    domains_file = config.get('main', 'domains_file')
    country_code = config.get('probe', 'country_code')

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

    ## Get the data
    logger.info('Getting Data')

    # Obtain a probe id using the first domain
    probe_tags = {'include': ['system-ipv4-works']}
    # Todo: In the future, allow the config to specify country, probe, etc...
    first_probe = [AtlasSource(type='country', value=country_code, requested=1, tags=probe_tags)]
    logger.info('Creating the first measurement')
    first_domain = domains[0]    
    first_measurement = [Ping(af=4, target=first_domain, description='Ping to ' + first_domain)]
    # Create first measurement
    success, first_request_ids = measurements.create_measurements(api_key, first_measurement, first_probe)
    logger.debug('Create first measurement success: ' + str(success))
    logger.debug('Results: ' + str(first_request_ids))
    if not success:
        logger.error('Error when creating measurement for the first domain')
        return None
    logger.debug('Retrieving measurments of id: ' + str(first_request_ids))
    # Get the probe id
    results = measurements.retrieve_measurement_results(first_request_ids, retrieve_measurements_timeout)
    first_result = next(iter(results.values()))[0]
    probe_id = first_result['prb_id']
    logger.debug('Probe: ' + str(probe_id))

    # Create the probe source
    probe_source = [AtlasSource(type='probes', value=str(probe_id), requested=1, tags=probe_tags)]

    # Create subsequent ping measurements
    logger.info('Creating ping measurements')
    ping_measurements = [Ping(af=4, target=domain, description='Ping to ' + domain) for domain in domains[1:]]
    final_ping_request_ids = first_request_ids
    success, request_ids = measurements.create_measurements(api_key, ping_measurements, probe_source)
    logger.debug('Create ping measurement success: ' + str(success))
    logger.debug('Ping results: ' + str(request_ids))
    final_ping_request_ids += request_ids
    # Write all final ping request ids to file
    logger.info('Storing ping ids')
    ping_req_id_file = open(ping_request_id_file, 'w+')
    for req_id in final_ping_request_ids:
        ping_req_id_file.write('%s \n' % req_id)
    ping_req_id_file.close()
    if not success:
        logger.error('Error when creating all measurements')
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
    success, final_dns_request_ids = measurements.create_measurements(api_key, dns_measurements, probe_source)
    logger.debug('Create dns measurement success: ' + str(success))
    logger.debug('Dns results: ' + str(final_dns_request_ids))
    # Write all final dns request ids to file
    logger.info('Storing dns ids')
    dns_req_id_file = open(dns_request_id_file, 'w+')
    for req_id in final_dns_request_ids:
        dns_req_id_file.write('%s \n' % req_id)
    dns_req_id_file.close()
    if not success:
        logger.error('Error when creating all dns measurements')
        return None
    # Get the results
    logger.info('Retrieving dns measurements')
    dns_results = measurements.retrieve_measurement_results(final_dns_request_ids, retrieve_measurements_timeout)
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
