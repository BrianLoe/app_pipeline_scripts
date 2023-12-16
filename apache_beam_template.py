import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    SetupOptions
)
import numpy as np

class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input', help='Path to the input file')
        parser.add_value_provider_argument('--output', help='Path to the output file')
        

class MinMaxScaler(beam.DoFn):
    def process(self, element):
        # Assuming 'element' is a list or tuple with feature values
        min_val = min(element)
        max_val = max(element)
        scaled_values = [(value - min_val) / (max_val - min_val) for value in element]
        yield scaled_values

class CalculateCosineSimilarity(beam.DoFn):
    def process(self, element):
        # Assuming 'element' is a list or tuple with scaled feature values
        unique_id, cluster_id, *feature_values = element

        # Convert the feature values to a NumPy array
        feature_array = np.array(feature_values)
        
        elements_in_cluster = element[1:] # get all elements within cluster

        # Calculate the cosine similarity within the cluster
        for other_element in elements_in_cluster:
            other_element_id, _, *other_features = other_element

            # Convert other features to a NumPy array
            other_feature_array = np.array(other_features)

            # Calculate cosine similarity
            dot_product = np.dot(feature_array, other_feature_array)
            norm_product = np.linalg.norm(feature_array) * np.linalg.norm(other_feature_array)
            
            # Avoid division by zero
            similarity = dot_product / norm_product if norm_product != 0 else 0.0

            # Output the result
            yield {
                'id': element_id,
                'other_id': other_element_id,
                'cluster_id': cluster_id,
                'cosine_similarity': similarity
            }

def run(argv=None, save_main_session=True):
    options = PipelineOptions()
    
    user_options = options.view_as(UserOptions)
    
    with beam.Pipeline(options=user_options) as p:
        # Read data from input source (e.g., text file, BigQuery)
        data = p | "ReadCSV" >> beam.io.ReadFromText(user_options.input, skip_header_lines=1)

        # Parse input data into dictionaries
        parsed_data = data | "ParseCSV" >> beam.Map(lambda x: [float(value) for value in x.split(',')])
        
        def restructure_columns(element):
            return element[:1] + element [-1:] + element[1:-1]
        
        # Restructure the columns in order
        restructured_data = parsed_data | "RestructureColumns" >> beam.Map(restructure_columns)

        # Apply Min-Max scaling to features
        scaled_data = restructured_data | "MinMaxScale" >> beam.ParDo(MinMaxScaler())

        # Group data by cluster (assuming 'cluster' is one of the keys in the dictionary)
        grouped_data = scaled_data | "GroupByCluster" >> beam.GroupBy(lambda x: int(x[1]))

        # Apply cosine similarity within each cluster
        similarity_results = grouped_data | "CosineSimilarity" >> beam.ParDo(CalculateCosineSimilarity())
        
        # Format results for CSV
        formatted_results = similarity_results | "FormatCSV" >> beam.Map(lambda x: ','.join(map(str, x)))

        # Write the results to an output path (e.g., text file)
        formatted_results | "WriteOutput" >> beam.io.WriteToText(user_options.output)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
