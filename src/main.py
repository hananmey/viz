import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText
import json
import argparse

# input & output args
parser = argparse.ArgumentParser()

parser.add_argument('--input1',
                    dest='input1',
                    required=True,
                    help='Input file1 to process.')

parser.add_argument('--input2',
                    dest='input2',
                    required=True,
                    help='Input file2 to process.')

parser.add_argument('--output',
                    dest='output',
                    required=True,
                    help='Output file to write results')

path_args, pipeline_args = parser.parse_known_args()
inputs_pattern1 = path_args.input1
inputs_pattern2 = path_args.input2
outputs_pattern = path_args.output


# class represents output model
class Output:
    def __init__(self, userid, event, institution, time):
        self.userid = userid
        self.event = event
        self.institution = institution
        self.time = time


# Converts line into dictionary
class line_to_dict(beam.DoFn):
    def process(self, element):
        return [json.loads(element)]


# format output data
class format_output(beam.DoFn):
    """Converts line into dictionary"""

    def process(self, element):
        userid = element[0]
        event = element[1]['users_data'][0][1]
        institution = (element[1]['institute_data'])[0][1]
        time = (element[1]['users_data'])[0][0]
        return [Output(userid, event, institution, time).__dict__]


# filter data contains 'instituion' element
def filter_institute(element):
    return 'instituion' in element


# declare pipeline
options = PipelineOptions(pipeline_args)
p = beam.Pipeline(options=options)

institute = (p
             | "READ FROM JSON" >> ReadFromText(inputs_pattern1)
             | 'TO_DICT' >>  beam.ParDo(line_to_dict())
             | beam.Filter(filter_institute)
             | 'KeyByType' >> beam.Map(lambda x: (x["user-id"], (x["time"], x["instituion"])))
             | 'GroupByType' >> beam.CombinePerKey(max)
             )

users = (p
         | "READ FROM JSON2" >> ReadFromText(inputs_pattern2)
         | 'TO_DICT_2' >> beam.ParDo(line_to_dict())
         | 'KeyByType2' >> beam.Map(lambda x: (x["user-id"], (x["time"], x["event"])))
         | 'GroupByType2' >> beam.CombinePerKey(max)
         )

# Join two colletions anf format output
res = ({'institute_data': institute, 'users_data': users}
       | beam.CoGroupByKey()
       | beam.ParDo(format_output())
       | beam.io.WriteToText(file_path_prefix=outputs_pattern)
       )

result = p.run()


