from os import environ

from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, MinHashLSH
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, collect_list

from utils.logger import SparkLogger


class EngineConfig(object):
    """
    Recommendation engine configuration
    """
    app_name = 'recommendation_engine'
    data_source_path = environ.get('INPUT_DATA_PATH', 'data/test-data-for-spark.json')
    recommendations_count = int(environ.get('RECOMMENDATIONS_COUNT', 10))
    master = environ.get('MASTER', 'local[*]')
    log_level = environ.get('LOG_LEVEL', 'WARN')


class RecommendationEngine(object):
    spark = None

    def __init__(self, config: EngineConfig):
        self.config = config
        self.spark = SparkSession\
            .builder\
            .appName(config.app_name)\
            .master(config.master)\
            .getOrCreate()

        self.logger = SparkLogger(self.spark, self.config.log_level).get_logger()

        self.attr_cols = None
        self.indexed_attr_cols = None
        self.encoded_attr_cols = None
        self.min_hash_model = None

        self.indexed_features_col_name = 'features_indexed'
        self.encoded_features_col_name = 'encoded_features'

        # dataset to train MinHash LSH model
        self.training_dataset = None

    def read_data(self):
        """
        Read products data
        :return: Dataframe
        """
        products_data = self.spark.read.json(self.config.data_source_path)
        return products_data.select(['sku', 'attributes.*'])

    def encode_data(self, products_data: DataFrame):
        """
        Encode string features to one-hot encoding to make them sparse vectors which will be used with MinHash algorithm
        to calculate the Jaccard distance between vectors
        :param products_data: Dataframe containing products features
        :return: encoded_data: Dataframe containing encoded data
        """
        # create list of column names for string features encoding
        self.attr_cols = list(filter(lambda col_name: col_name.startswith('att'), products_data.columns))
        self.indexed_attr_cols = list(map(lambda col_name: f'indexed-{col_name}', self.attr_cols))
        self.encoded_attr_cols = list(map(lambda col_name: f'encoded-{col_name}', self.attr_cols))

        # Encode string values to index numbers
        indexer = StringIndexer(inputCols=self.attr_cols, outputCols=self.indexed_attr_cols)
        indexer_model = indexer.fit(products_data)
        indexed_data = indexer_model.transform(products_data)

        # encode indexed features to one-hot encoding
        encoder = OneHotEncoder(inputCols=self.indexed_attr_cols, outputCols=self.encoded_attr_cols)
        encoder_model = encoder.fit(indexed_data)
        encoded_data = encoder_model.transform(indexed_data)

        return encoded_data

    def assemble_features(self, encoded_data: DataFrame):
        """
        Assemble features into a vector which will be used to train MinHash model for Jaccard distance
        :param encoded_data:
        :return: DataFrame
        """
        # indexed features will be used later to rank up products in case of draw
        indexed_features_assembler = VectorAssembler(inputCols=self.indexed_attr_cols,
                                                     outputCol=self.indexed_features_col_name)

        # one hot encoded features will be used to train minhash LSH model
        encoded_features_assembler = VectorAssembler(inputCols=self.encoded_attr_cols,
                                                     outputCol=self.encoded_features_col_name)

        features = indexed_features_assembler.transform(encoded_data)
        return encoded_features_assembler.transform(features)

    def train_minhash_model(self, encoded_features_data, feature_hashes_col_name='product_feature_hashes'):
        """
        Train MinHASH LSH model
        :param feature_hashes_col_name: Ouput column name for where hashes will be added by MinHash algorithm
        :param encoded_features_data: one-hot encoded data in the form of assembled vectors
        :return: None
        """
        self.training_dataset = encoded_features_data.select(['sku', self.encoded_features_col_name])
        min_hash = MinHashLSH(inputCol=self.encoded_features_col_name,
                              outputCol=feature_hashes_col_name,
                              numHashTables=len(self.encoded_attr_cols))

        self.min_hash_model = min_hash.fit(self.training_dataset)

    def get_search_key_features(self, product_sku):
        """
        Returns features vector for the provided product SKU
        :param product_sku: product SKU
        :return: SparseVector
        """
        if not self.training_dataset:
            raise Exception('Please execute train_minhash_model method before executing this method')

        product_encoded_features = self.training_dataset\
            .filter(col('sku') == product_sku)\
            .select(self.encoded_features_col_name)\
            .head()

        if not product_encoded_features:
            raise Exception(f'No product exists with the provided product SKU: {product_sku}')
        return product_encoded_features.encoded_features

    def calculate_jaccard_distance(self, key, distance_col_name='Jaccard_distance'):
        """
        Calculate Jaccard distance between product features
        :param key: product SKU for getting recommendations
        :param distance_col_name: column name in the output dataframe
        :return: DataFrame
        """
        return self.min_hash_model.approxNearestNeighbors(self.training_dataset,
                                                          key,
                                                          # one match will be with the same product so make it n + 1
                                                          self.config.recommendations_count + 1,
                                                          distCol=distance_col_name)

    @staticmethod
    def _calculate_weights_difference(input_product_features, products_features_indexed):
        """
        Calculate difference between input product features and other products which are showing draw situation
        :param input_product_features: Features vector created from StringIndexer
        :param products_features_indexed: Features vector created from StringIndexer
        :return: DenseVector
        """
        weights_difference = {}

        for product_indexed_record in products_features_indexed:
            weights_difference[product_indexed_record.sku] = list(
                input_product_features.features_indexed - product_indexed_record.features_indexed)

        return weights_difference

    def _rank_products(self, weights_difference_data):
        """
        Rank products based on the indexed features data
        :param weights_difference_data: Difference of indexed features with the key product to identify matching weights
        :return: List
        """
        weights_difference = weights_difference_data.copy()

        ranked_products = []
        while weights_difference:
            if len(weights_difference) == 1:
                sku, features_indexed_vector = list(weights_difference.items()).pop()
                self.logger.debug(f'{sku}:{features_indexed_vector}')
                ranked_products.append(sku)
                break

            weights_diff_values = list(weights_difference.values())
            weights_diff_keys = list(weights_difference.keys())

            for weights in zip(*weights_diff_values):
                if 0.0 in weights:
                    if len(set(weights)) == 1:
                        continue
                    else:
                        self.logger.debug(f'weights: {weights}')
                        vector_index = weights.index(0.0)
                        self.logger.debug(f'vector_index:{vector_index}')
                        ranked_up_sku = weights_diff_keys[vector_index]
                        features_indexed_vector = weights_difference.pop(ranked_up_sku)
                        self.logger.debug(f'{ranked_up_sku}:{features_indexed_vector}')
                        ranked_products.append(ranked_up_sku)
                        break

        return ranked_products

    def rank_recommendations(self, key_product_name, features_jaccard_distance, features_data,
                             distance_col_name='jaccard_distance'):
        """
        Rank product recommendations by indexed features
        :param key_product_name: product SKU
        :param features_jaccard_distance: dataframe containing jaccard distance values for products
        :param features_data: sparse features vector
        :param distance_col_name: jaccard distance column name in the provided features_jaccard_distance
        :return: List
        """
        # group products by same distance values for ranking
        aggregated_jaccard_distance = features_jaccard_distance\
            .groupBy(distance_col_name)\
            .agg(collect_list('sku').alias('products_with_same_distance'))\
            .collect()

        # get indexed features of input product name
        input_product_indexed_features = features_data\
            .filter(col('sku') == key_product_name)\
            .select(['sku', self.indexed_features_col_name])\
            .head()

        ranked_product_sku = []
        rank = 0
        for product_jaccard_distance in aggregated_jaccard_distance:
            product_names = product_jaccard_distance.products_with_same_distance
            if len(product_names) == 1:
                # ignore the ranking of the input product name
                if product_names[0] == key_product_name:
                    continue

                rank += 1
                ranked_product_sku.append((rank, product_names[0], product_jaccard_distance.jaccard_distance))
            else:
                product_indexed_data = features_data\
                    .filter(col('sku').isin(product_names))\
                    .select(['sku', self.indexed_features_col_name])\
                    .collect()
                weights_difference = RecommendationEngine._calculate_weights_difference(input_product_indexed_features,
                                                                                        product_indexed_data)
                ranked_products = self._rank_products(weights_difference)
                for ranked_product in ranked_products:
                    if product_names[0] == key_product_name:
                        continue
                    rank += 1
                    ranked_product_sku.append((rank, ranked_product, product_jaccard_distance.jaccard_distance))

        return ranked_product_sku

    def run(self, product_sku):
        """
        Execute recommendation engine logic and provide products based on ranking
        :param product_sku: string containing product sku i.e. 'sku-1'
        :return: DataFrame
        """
        products_data = self.read_data()
        encoded_data = self.encode_data(products_data)
        features = self.assemble_features(encoded_data)
        self.train_minhash_model(features)

        input_product_features = self.get_search_key_features(product_sku)

        features_jaccard_distance = self.calculate_jaccard_distance(input_product_features)
        ranked_results = self.rank_recommendations(product_sku, features_jaccard_distance, features)
        ranked_products_df = self.spark.createDataFrame(ranked_results, ['ranking', 'product_sku', 'jaccard_distance'])

        return ranked_products_df
