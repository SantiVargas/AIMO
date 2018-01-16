import time
from datetime import datetime
import pickle
from ripe.atlas.cousteau import AtlasSource, Ping, Dns, AtlasCreateRequest, AtlasResultsRequest, Measurement, MeasurementRequest
from ripe.atlas.sagan import PingResult, DnsResult
# Logger
import logging
logger = logging.getLogger('main_logger')

class MyMeasurementRequest(MeasurementRequest):
    url = "/api/v2/measurements/my/"

def create_measurements(api_key, measurements, sources):
    success_list = []
    request_ids = []

    logger.info('Chunking measurements')
    chunk_size = 100
    measurement_chunks = [measurements[x:x+chunk_size] for x in range(0, len(measurements), chunk_size)]
    for i, measurement_chunk in enumerate(measurement_chunks):
        # Check if there are simultaneous (concurrently running) measurements before running the next chunk
        concurrent_status = '1,2'
        concurrent_measurements = [x['id'] for x in MyMeasurementRequest(**{'status': concurrent_status, 'key': api_key})]
        while concurrent_measurements:
            logger.info('Found ' + str(len(concurrent_measurements)) + ' concurrent measurements. Sleeping')
            logger.debug(concurrent_measurements)
            time.sleep(60)
            concurrent_measurements = [x['id'] for x in MyMeasurementRequest(**{'status': concurrent_status, 'key': api_key})]

        logger.info('-Creating measurements for measurement chunk ' + str(i))
        for msm in measurement_chunk:
            logger.debug('Creating: ' + str(msm))
            atlas_request = AtlasCreateRequest(
                start_time=datetime.utcnow(),
                key=api_key,
                is_oneoff=True,
                measurements=[msm],
                sources=sources,
            )
            success, response = atlas_request.create()
            logger.debug('Creating success: ' + str(success))
            logger.debug('Creating response: ' + str(response))
            if not success:
                logger.error('Unsuccessful creating measurements for ' + str(msm))
                logger.error('Failure response:' + str(response))
                # return False, request_ids
            else:
                request_ids.extend(response['measurements'])
                success_list.append(msm)

    return success_list, request_ids

def retrieve_measurement_results(measurement_ids, polling_interval):
    ## Retrieve measurement results

    # Copy the list
    pending_measurements = list(measurement_ids)

    results = dict()

    i = 0
    logger.debug('Polling through ids')
    cur_length = len(pending_measurements)
    logger.info('Initial Pending Domains Length: ' + str(cur_length))
    # Keep polling while there are pending results
    while pending_measurements:
        logger.debug('Polling iteration: ' + str(i))
        for m_id in pending_measurements:
            logger.debug('Polling id: ' + str(m_id))
            is_success, response = AtlasResultsRequest(msm_id=m_id).create()
            logger.debug('Polling success: ' + str(is_success))
            logger.debug('Polling response: ' + str(response))
            if is_success and response:
                logger.debug('Good response...')
                pending_measurements.remove(m_id)
                results[m_id] = response

        if len(pending_measurements) != cur_length:
            cur_length = len(pending_measurements)
            logger.info('New Pending Domains Length: ' + str(cur_length))

        # Minor optimization to skip sleeping if unnecessary
        if pending_measurements:
            time.sleep(polling_interval)
            i += 1

    return results
