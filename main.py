import argparse

from utils.recommendation_engine import RecommendationEngine, EngineConfig

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Products Recommendation Engine')
    parser.add_argument('product_sku', help='Product SKU string for getting recommendations.')
    args = parser.parse_args()

    r_engine = RecommendationEngine(EngineConfig)
    result = r_engine.run(args.product_sku)
    print(f'Top {EngineConfig.recommendations_count} Recommendations:')
    result.show()
