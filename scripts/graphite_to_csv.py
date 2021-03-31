import requests
from argparse import ArgumentParser
import yaml


DEFAULT_GRAPHITE_PORT=80
DATAPOINT_VALUE_INDEX = 0
DATAPOINT_TS_INDEX = 1


def fetch_from_graphite(host, port, query, from_seconds, destination):
    r = requests.get(f'http://{host}:{port}/render/', params={'target': query, 'from': f'-{from_seconds}sec', 'format': 'json'})
    csv = []
    for entry in r.json():
        entity = entry['target']
        for datapoint in entry['datapoints']:
            timestamp = datapoint[DATAPOINT_TS_INDEX]
            value = datapoint[DATAPOINT_VALUE_INDEX]
            value = 'NaN' if value is None else value
            csv.append(f'{entity},{timestamp},{value}')
    with open(destination, 'w+') as output:
        output.write('\n'.join(csv))


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('--experiment', help='path to experiment YAML', type=str, required=True)
    parser.add_argument('--destination', help='destination folder', type=str, required=True)
    parser.add_argument('--duration', help='Graphite query window (seconds)', required=True)
    parser.add_argument('--host', help='Graphite hostname/IP')
    parser.add_argument('--port', help='Graphite UI port', default=DEFAULT_GRAPHITE_PORT)
    args = parser.parse_args()
    with open(args.experiment, 'r') as file:
        conf = yaml.load('\n'.join(file.readlines()), Loader=yaml.CLoader)
        if 'csv_queries' not in conf:
            raise ValueError('No csv_queries entry in experiment YAML!')
        csv_queries = conf['csv_queries']
        for (name, query) in csv_queries.items():
            fetch_from_graphite(args.host, args.port, query, args.duration, f'{args.destination}/{name}.csv')

