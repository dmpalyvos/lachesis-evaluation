import yaml
import argparse
from copy import copy


PARAMTER_CONFIG_KEY='dimensions'


parser = argparse.ArgumentParser()
parser.add_argument('INPUT', help='The path to the input template YAML file')
parser.add_argument('OUTPUT', help='The path to the output YAML file')
args = parser.parse_args()

with open(args.INPUT, 'r') as file:
    conf = yaml.load('\n'.join(file.readlines()), Loader=yaml.CLoader)
    schema = conf[PARAMTER_CONFIG_KEY]['schema']
    for i, parameter in enumerate(schema.split('.')):
        if i == 0:
            # Variants are already defined by user
            assert parameter == 'variant', 'First schema parameter should be "variant"'
            continue
        newVariants = []
        for variant in conf['variants']:
            for value in conf[PARAMTER_CONFIG_KEY][parameter]:
                configuredVariant = copy(variant)
                configuredVariant['args'] += f' --{parameter} {value} '
                configuredVariant['name'] += f'.{value}'
                configuredVariant[parameter] = value
                newVariants.append(configuredVariant)
        conf['variants'] = newVariants
    print(f'# {len(conf["variants"])} total runs')
    with open(args.OUTPUT, 'w+') as output_file:
        yaml.safe_dump(conf, output_file)
